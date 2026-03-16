// log-parser: native nginx log parser for log-analyser.
//
// Replaces the pure-Python build_parsed_for_date() hot path.
// Reads nginx combined-format log files + YAML config and writes a
// newline-delimited JSON (NDJSON) file.  Python reads the NDJSON file,
// fixes the timestamp column types, and writes the final parquet output.
//
// Output format notes:
//   - ts_local: ISO-8601 string with original timezone offset, e.g.
//               "2024-01-01T10:30:00+08:00".  Python converts with
//               pd.to_datetime(col, utc=True) → TIMESTAMPTZ parquet column.
//   - ts_utc:   ISO-8601 string in UTC, no timezone suffix, e.g.
//               "2024-01-01T02:30:00".  Python converts with
//               pd.to_datetime(col) → naive TIMESTAMP parquet column.
//   - Nullable fields:  JSON null.
//
// Usage:
//
//	log-parser --date 2024-01-01 --out /path/to/parsed.ndjson \
//	           --bots /path/to/bots.yml --urls /path/to/url_groups.yml \
//	           /path/to/logfile1.log [/path/to/logfile2.log ...]
//
// The binary writes stats to stderr and the row count as a single integer to
// stdout so the Python caller can capture it cheaply.
package main

import (
	"bufio"
	"encoding/json"
	"flag"
	"fmt"
	"net/url"
	"os"
	"regexp"
	"strconv"
	"strings"
	"time"

	"gopkg.in/yaml.v3"
)

// ---------------------------------------------------------------------------
// Constants
// ---------------------------------------------------------------------------

const nginxTimeLayout = "02/Jan/2006:15:04:05 -0700"
const noLocaleLabel = "no-locale"

// ---------------------------------------------------------------------------
// Nginx log regex – mirrors Python's NGINX_RE exactly.
// ---------------------------------------------------------------------------

var nginxRE = regexp.MustCompile(
	`^(?P<ip>\S+)\s+\S+\s+(?:(?P<country>[A-Z]{2})\s+)?\S+\s+\[(?P<time>[^\]]+)\]\s+` +
		`(?:-\s+)?` +
		`"(?P<request>[^"]*)"\s+` +
		`(?P<status>\d{3})\s+` +
		`(?P<bytes>\d+)\s+` +
		`"(?P<referer>[^"]*)"\s+` +
		`"(?P<ua>[^"]*)"\s*$`,
)

// pre-computed named-group indices
var nginxIdx = func() map[string]int {
	m := make(map[string]int)
	for i, name := range nginxRE.SubexpNames() {
		if name != "" {
			m[name] = i
		}
	}
	return m
}()

// ---------------------------------------------------------------------------
// YAML config types
// ---------------------------------------------------------------------------

type botsYAML struct {
	Rules        []botRuleYAML `yaml:"rules"`
	RefererRules []botRuleYAML `yaml:"referer_rules"`
}

type botRuleYAML struct {
	Family  string `yaml:"family"`
	Pattern string `yaml:"pattern"`
}

type urlGroupsYAML struct {
	Locales                []string          `yaml:"locales"`
	Rules                  []urlRuleYAML     `yaml:"rules"`
	SectionMap             map[string]string `yaml:"section_map"`
	FallbackGroup          string            `yaml:"fallback_group"`
	LocaleHomepageGroup    string            `yaml:"locale_homepage_group"`
	LocaleHomepagePattern  string            `yaml:"locale_homepage_pattern"`
}

type urlRuleYAML struct {
	Group string      `yaml:"group"`
	Match string      `yaml:"match"`
	Value interface{} `yaml:"value"` // string or []interface{} for "ext"
}

// ---------------------------------------------------------------------------
// Runtime types
// ---------------------------------------------------------------------------

type botRule struct {
	family  string
	pattern *regexp.Regexp
}

type urlRule struct {
	group    string
	match    string // "exact" | "prefix" | "regex" | "ext"
	value    string // lower-cased value for exact / prefix / regex
	extSet   map[string]struct{}
	compiled *regexp.Regexp
}

type urlConfig struct {
	locales                map[string]struct{}
	rules                  []urlRule
	sectionMap             map[string]string
	fallbackGroup          string
	localeHomepageGroup    string
	localeHomepagePattern  *regexp.Regexp // fallback for unlisted locale-like segments
}

// ---------------------------------------------------------------------------
// NDJSON row type.
//
// Pointer fields → JSON null when nil (nullable columns in parquet).
// ts_local / ts_utc are emitted as ISO-8601 strings.
// ---------------------------------------------------------------------------

type logRowJSON struct {
	Date            string  `json:"date"`
	TsLocal         string  `json:"ts_local"`
	TsUtc           string  `json:"ts_utc"`
	EdgeIP          string  `json:"edge_ip"`
	Country         *string `json:"country"`
	Method          *string `json:"method"`
	Path            string  `json:"path"`
	HttpVersion     *string `json:"http_version"`
	Status          int32   `json:"status"`
	StatusClass     int32   `json:"status_class"`
	BytesSent       int64   `json:"bytes_sent"`
	Referer         *string `json:"referer"`
	UserAgent       string  `json:"user_agent"`
	HasQuery        bool    `json:"has_query"`
	IsParameterized bool    `json:"is_parameterized"`
	Locale          string  `json:"locale"`
	Section         *string `json:"section"`
	UrlGroup        string  `json:"url_group"`
	IsResource      bool    `json:"is_resource"`
	IsBot           bool    `json:"is_bot"`
	BotFamily       *string `json:"bot_family"`
	RequestTarget   string  `json:"request_target"`
	QueryString     *string `json:"query_string"`
	IsUtmChatgpt    bool    `json:"is_utm_chatgpt"`
	HasUtm          bool    `json:"has_utm"`
	UtmSource       *string `json:"utm_source"`
	UtmSourceNorm   *string `json:"utm_source_norm"`
	UtmMedium       *string `json:"utm_medium"`
	UtmCampaign     *string `json:"utm_campaign"`
	UtmTerm         *string `json:"utm_term"`
	UtmContent      *string `json:"utm_content"`
}

// ---------------------------------------------------------------------------
// Config loaders
// ---------------------------------------------------------------------------

func loadBotRules(path string) ([]botRule, []botRule, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, nil, err
	}
	var cfg botsYAML
	if err := yaml.Unmarshal(data, &cfg); err != nil {
		return nil, nil, err
	}

	var rules []botRule
	for _, r := range cfg.Rules {
		pat, err := regexp.Compile("(?i)" + r.Pattern)
		if err != nil {
			return nil, nil, fmt.Errorf("bad bot pattern %q: %w", r.Pattern, err)
		}
		rules = append(rules, botRule{family: r.Family, pattern: pat})
	}

	var refRules []botRule
	for _, r := range cfg.RefererRules {
		pat, err := regexp.Compile("(?i)" + r.Pattern)
		if err != nil {
			return nil, nil, fmt.Errorf("bad referer pattern %q: %w", r.Pattern, err)
		}
		refRules = append(refRules, botRule{family: r.Family, pattern: pat})
	}

	return rules, refRules, nil
}

func loadURLConfig(path string) (*urlConfig, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	var cfg urlGroupsYAML
	if err := yaml.Unmarshal(data, &cfg); err != nil {
		return nil, err
	}

	locales := make(map[string]struct{})
	for _, l := range cfg.Locales {
		l = strings.TrimSpace(strings.ToLower(l))
		if l != "" {
			locales[l] = struct{}{}
		}
	}

	var rules []urlRule
	for _, r := range cfg.Rules {
		ur := urlRule{group: r.Group, match: r.Match}
		switch r.Match {
		case "exact", "prefix":
			ur.value = strings.ToLower(fmt.Sprintf("%v", r.Value))
		case "regex":
			s := fmt.Sprintf("%v", r.Value)
			ur.value = s
			pat, err := regexp.Compile("(?i)" + s)
			if err != nil {
				return nil, fmt.Errorf("bad url regex %q: %w", s, err)
			}
			ur.compiled = pat
		case "ext":
			ur.extSet = make(map[string]struct{})
			switch v := r.Value.(type) {
			case []interface{}:
				for _, e := range v {
					ur.extSet[strings.ToLower(fmt.Sprintf("%v", e))] = struct{}{}
				}
			case string:
				ur.extSet[strings.ToLower(v)] = struct{}{}
			}
		}
		rules = append(rules, ur)
	}

	sectionMap := make(map[string]string)
	for k, v := range cfg.SectionMap {
		sectionMap[strings.ToLower(k)] = v
	}

	fallback := cfg.FallbackGroup
	if fallback == "" {
		fallback = "Other Content"
	}

	localeHomepageGroup := cfg.LocaleHomepageGroup
	if localeHomepageGroup == "" {
		localeHomepageGroup = "Locale Homepage"
	}

	var localeHomepagePattern *regexp.Regexp
	if cfg.LocaleHomepagePattern != "" {
		pat, err := regexp.Compile("(?i)" + cfg.LocaleHomepagePattern)
		if err != nil {
			return nil, fmt.Errorf("bad locale_homepage_pattern %q: %w", cfg.LocaleHomepagePattern, err)
		}
		localeHomepagePattern = pat
	}

	return &urlConfig{
		locales:               locales,
		rules:                 rules,
		sectionMap:            sectionMap,
		fallbackGroup:         fallback,
		localeHomepageGroup:   localeHomepageGroup,
		localeHomepagePattern: localeHomepagePattern,
	}, nil
}

// ---------------------------------------------------------------------------
// Bot classification
// ---------------------------------------------------------------------------

func classifyBot(ua string, rules []botRule) (isBot bool, family string) {
	uaL := strings.ToLower(ua)
	for _, r := range rules {
		if r.pattern.MatchString(uaL) {
			return r.family != "Browser/Other", r.family
		}
	}
	return false, "Browser/Other"
}

// ---------------------------------------------------------------------------
// URL grouping – mirrors Python apply_url_grouping() exactly.
// ---------------------------------------------------------------------------

func extOfPath(p string) string {
	seg := p
	if i := strings.LastIndex(p, "/"); i >= 0 {
		seg = p[i+1:]
	}
	if i := strings.LastIndex(seg, "."); i >= 0 {
		return strings.ToLower(seg[i+1:])
	}
	return ""
}

func normalizePathOnly(p string) string {
	if p == "" {
		p = "/"
	}
	if i := strings.Index(p, "?"); i >= 0 {
		p = p[:i]
	}
	if !strings.HasPrefix(p, "/") {
		p = "/" + p
	}
	p = strings.ToLower(p)
	if p == "" {
		p = "/"
	}
	return p
}

func pathSegments(p string) []string {
	var segs []string
	for _, s := range strings.Split(p, "/") {
		if s != "" {
			segs = append(segs, s)
		}
	}
	return segs
}

// applyURLGrouping returns (group, locale, section).
// locale is always non-empty: either a real locale code or "no-locale".
// section may be nil.
func applyURLGrouping(path string, cfg *urlConfig) (group, locale string, section *string) {
	p := normalizePathOnly(path)

	for _, r := range cfg.rules {
		switch r.match {
		case "exact":
			if p == r.value {
				return r.group, noLocaleLabel, nil
			}
		case "prefix":
			if strings.HasPrefix(p, r.value) {
				return r.group, noLocaleLabel, nil
			}
		case "regex":
			if r.compiled != nil && r.compiled.MatchString(p) {
				return r.group, noLocaleLabel, nil
			}
		case "ext":
			if ext := extOfPath(p); ext != "" {
				if _, ok := r.extSet[ext]; ok {
					return r.group, noLocaleLabel, nil
				}
			}
		}
	}

	segs := pathSegments(p)
	if len(segs) == 0 {
		return "Home", noLocaleLabel, nil
	}

	first := segs[0]
	var detectedLocale string
	var sectionIdx int

	if _, ok := cfg.locales[first]; ok {
		detectedLocale = first
		if len(segs) == 1 {
			return cfg.localeHomepageGroup, detectedLocale, nil
		}
		sectionIdx = 1
	} else if len(segs) == 1 && cfg.localeHomepagePattern != nil && cfg.localeHomepagePattern.MatchString(first) {
		// Single-segment path whose segment looks like a locale code but isn't
		// in the explicit whitelist (e.g. /en-gb, /ja, /zh-tw).  Classify as
		// Locale Homepage so these don't pollute Other Content.
		return cfg.localeHomepageGroup, first, nil
	} else {
		detectedLocale = noLocaleLabel
		sectionIdx = 0
	}

	var sectionStr *string
	if len(segs) > sectionIdx {
		s := segs[sectionIdx]
		sectionStr = &s

		// Composite key (section/subsection) checked first.
		if len(segs) > sectionIdx+1 {
			composite := strings.ToLower(s + "/" + segs[sectionIdx+1])
			if mapped, ok := cfg.sectionMap[composite]; ok {
				return mapped, detectedLocale, sectionStr
			}
		}
		if mapped, ok := cfg.sectionMap[strings.ToLower(s)]; ok {
			return mapped, detectedLocale, sectionStr
		}
	}

	return cfg.fallbackGroup, detectedLocale, sectionStr
}

// ---------------------------------------------------------------------------
// Request line parsing – mirrors Python parse_request().
// ---------------------------------------------------------------------------

func parseRequest(req string) (method *string, path string, httpVersion *string, hasQuery bool, requestTarget string, queryString *string) {
	path = "/"
	requestTarget = "/"

	if req == "" {
		return
	}
	parts := strings.SplitN(req, " ", 3)
	if len(parts) < 2 {
		return
	}

	m := parts[0]
	method = &m

	rt := parts[1]
	if rt == "" {
		rt = "/"
	}
	requestTarget = rt
	path = rt

	if len(parts) >= 3 {
		hv := parts[2]
		httpVersion = &hv
	}

	if i := strings.Index(rt, "?"); i >= 0 {
		path = rt[:i]
		if path == "" {
			path = "/"
		}
		qs := rt[i+1:]
		if qs != "" {
			queryString = &qs
		}
		hasQuery = true
	}
	return
}

// ---------------------------------------------------------------------------
// Status class
// ---------------------------------------------------------------------------

func statusClass(s int32) int32 {
	switch {
	case s >= 200 && s <= 299:
		return 2
	case s >= 300 && s <= 399:
		return 3
	case s >= 400 && s <= 499:
		return 4
	case s >= 500 && s <= 599:
		return 5
	default:
		return 0
	}
}

// ---------------------------------------------------------------------------
// UTM parsing – mirrors Python parse_utm_fields() + helpers.
// ---------------------------------------------------------------------------

// doubleDecodeQuery applies url.QueryUnescape twice, mirroring Python's
// _safe_unquote_plus(s, passes=2). url.ParseQuery then applies a third pass.
func doubleDecodeQuery(s string) string {
	d1, err := url.QueryUnescape(s)
	if err != nil {
		return s
	}
	d2, err := url.QueryUnescape(d1)
	if err != nil {
		return d1
	}
	return d2
}

func firstQSValue(vals url.Values, key string) string {
	for _, k := range []string{key, strings.ToUpper(key)} {
		if v, ok := vals[k]; ok && len(v) > 0 {
			return v[0]
		}
	}
	return ""
}

func normalizeUTMValue(s string) *string {
	s = strings.TrimSpace(s)
	if s == "" {
		return nil
	}
	return &s
}

type utmResult struct {
	hasUTM        bool
	utmSource     *string
	utmSourceNorm *string
	utmMedium     *string
	utmCampaign   *string
	utmTerm       *string
	utmContent    *string
}

func parseUTMFields(queryString string) utmResult {
	if queryString == "" {
		return utmResult{}
	}

	decoded := doubleDecodeQuery(queryString)
	vals, err := url.ParseQuery(decoded)
	if err != nil {
		return utmResult{}
	}

	hasUTM := false
	for k := range vals {
		if strings.HasPrefix(strings.ToLower(k), "utm_") {
			hasUTM = true
			break
		}
	}
	if !hasUTM {
		return utmResult{}
	}

	rawSource := firstQSValue(vals, "utm_source")
	utmSource := normalizeUTMValue(rawSource)

	var utmSourceNorm *string
	if utmSource != nil {
		norm := strings.ToLower(*utmSource)
		utmSourceNorm = &norm
	}

	return utmResult{
		hasUTM:        true,
		utmSource:     utmSource,
		utmSourceNorm: utmSourceNorm,
		utmMedium:     normalizeUTMValue(firstQSValue(vals, "utm_medium")),
		utmCampaign:   normalizeUTMValue(firstQSValue(vals, "utm_campaign")),
		utmTerm:       normalizeUTMValue(firstQSValue(vals, "utm_term")),
		utmContent:    normalizeUTMValue(firstQSValue(vals, "utm_content")),
	}
}

// ---------------------------------------------------------------------------
// Core processing loop
// ---------------------------------------------------------------------------

type uaCacheEntry struct {
	isBot  bool
	family string
}

type pathCacheEntry struct {
	group   string
	locale  string
	section *string
}

func processFiles(date string, files []string, botRules, refererRules []botRule, cfg *urlConfig, out *bufio.Writer) (nRows, nBad, nNoLocale int) {
	enc := json.NewEncoder(out)

	uaCache := make(map[string]uaCacheEntry)
	pathCache := make(map[string]pathCacheEntry)

	for _, fp := range files {
		f, err := os.Open(fp)
		if err != nil {
			fmt.Fprintf(os.Stderr, "warning: cannot open %s: %v\n", fp, err)
			continue
		}

		// 1 MB scanner buffer – handles very long UA strings.
		scanner := bufio.NewScanner(f)
		scanner.Buffer(make([]byte, 1<<20), 1<<20)

		for scanner.Scan() {
			line := scanner.Text()

			m := nginxRE.FindStringSubmatch(line)
			if m == nil {
				nBad++
				continue
			}

			ip := m[nginxIdx["ip"]]
			timeStr := m[nginxIdx["time"]]
			req := m[nginxIdx["request"]]
			statusStr := m[nginxIdx["status"]]
			bytesStr := m[nginxIdx["bytes"]]
			referer := m[nginxIdx["referer"]]
			ua := m[nginxIdx["ua"]]

			var countryPtr *string
			if ci, ok := nginxIdx["country"]; ok && m[ci] != "" {
				c := m[ci]
				countryPtr = &c
			}

			statusInt, err := strconv.ParseInt(statusStr, 10, 32)
			if err != nil {
				nBad++
				continue
			}
			status := int32(statusInt)

			bytesInt, err := strconv.ParseInt(bytesStr, 10, 64)
			if err != nil {
				nBad++
				continue
			}

			tsLocal, err := time.Parse(nginxTimeLayout, timeStr)
			if err != nil {
				nBad++
				continue
			}
			tsUTC := tsLocal.UTC()

			method, path, httpVer, hasQuery, requestTarget, queryString := parseRequest(req)

			var refererPtr *string
			if referer != "" && referer != "-" {
				refererPtr = &referer
			}

			// Bot classification (cached by UA string).
			uce, ok := uaCache[ua]
			if !ok {
				isBot, family := classifyBot(ua, botRules)
				uce = uaCacheEntry{isBot: isBot, family: family}
				uaCache[ua] = uce
			}
			isBot := uce.isBot
			botFamily := uce.family

			// Referer-based bot check (not cached – same UA can arrive with different referers).
			if !isBot && refererPtr != nil && len(refererRules) > 0 {
				refL := strings.ToLower(*refererPtr)
				for _, rr := range refererRules {
					if rr.pattern.MatchString(refL) {
						isBot = true
						botFamily = rr.family
						break
					}
				}
			}

			// URL grouping (cached by path).
			pce, ok := pathCache[path]
			if !ok {
				group, locale, section := applyURLGrouping(path, cfg)
				pce = pathCacheEntry{group: group, locale: locale, section: section}
				pathCache[path] = pce
			}

			// UTM parsing.
			var qsStr string
			if queryString != nil {
				qsStr = *queryString
			}
			utm := parseUTMFields(qsStr)

			isResource := pce.group == "Nuxt Assets" || pce.group == "Static Assets"

			var botFamilyPtr *string
			if isBot {
				botFamilyPtr = &botFamily
			}

			isUTMChatgpt := utm.utmSourceNorm != nil && *utm.utmSourceNorm == "chatgpt.com"

			// ts_local: keep original offset so Python/pandas preserves the
			// wall-clock timezone when converting to TIMESTAMPTZ.
			// ts_utc: naive UTC string (no suffix) → pandas reads as tz-naive TIMESTAMP.
			row := logRowJSON{
				Date:            date,
				TsLocal:         tsLocal.Format("2006-01-02T15:04:05Z07:00"),
				TsUtc:           tsUTC.Format("2006-01-02T15:04:05"),
				EdgeIP:          ip,
				Country:         countryPtr,
				Method:          method,
				Path:            path,
				HttpVersion:     httpVer,
				Status:          status,
				StatusClass:     statusClass(status),
				BytesSent:       bytesInt,
				Referer:         refererPtr,
				UserAgent:       ua,
				HasQuery:        hasQuery,
				IsParameterized: hasQuery,
				Locale:          pce.locale,
				Section:         pce.section,
				UrlGroup:        pce.group,
				IsResource:      isResource,
				IsBot:           isBot,
				BotFamily:       botFamilyPtr,
				RequestTarget:   requestTarget,
				QueryString:     queryString,
				IsUtmChatgpt:    isUTMChatgpt,
				HasUtm:          utm.hasUTM,
				UtmSource:       utm.utmSource,
				UtmSourceNorm:   utm.utmSourceNorm,
				UtmMedium:       utm.utmMedium,
				UtmCampaign:     utm.utmCampaign,
				UtmTerm:         utm.utmTerm,
				UtmContent:      utm.utmContent,
			}

			if err := enc.Encode(row); err != nil {
				fmt.Fprintf(os.Stderr, "warning: json encode error: %v\n", err)
				nBad++
				continue
			}

			nRows++
			if pce.locale == noLocaleLabel {
				nNoLocale++
			}
		}

		f.Close()
		if err := scanner.Err(); err != nil {
			fmt.Fprintf(os.Stderr, "warning: read error in %s: %v\n", fp, err)
		}
	}

	return nRows, nBad, nNoLocale
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

func commaSep(n int) string {
	s := strconv.Itoa(n)
	out := make([]byte, 0, len(s)+len(s)/3)
	for i := range s {
		if i > 0 && (len(s)-i)%3 == 0 {
			out = append(out, ',')
		}
		out = append(out, s[i])
	}
	return string(out)
}

// ---------------------------------------------------------------------------
// main
// ---------------------------------------------------------------------------

func main() {
	date := flag.String("date", "", "Log date (YYYY-MM-DD) [required]")
	outPath := flag.String("out", "", "Output NDJSON file path [required]")
	botsYML := flag.String("bots", "", "Path to bots.yml [required]")
	urlsYML := flag.String("urls", "", "Path to url_groups.yml [required]")
	flag.Parse()

	if *date == "" || *outPath == "" || *botsYML == "" || *urlsYML == "" {
		fmt.Fprintln(os.Stderr,
			"usage: log-parser --date DATE --out OUT --bots bots.yml --urls url_groups.yml FILE...")
		os.Exit(1)
	}

	files := flag.Args()
	if len(files) == 0 {
		fmt.Fprintln(os.Stderr, "error: no input files specified")
		os.Exit(1)
	}

	botRules, refererRules, err := loadBotRules(*botsYML)
	if err != nil {
		fmt.Fprintf(os.Stderr, "error loading bots.yml: %v\n", err)
		os.Exit(1)
	}

	urlCfg, err := loadURLConfig(*urlsYML)
	if err != nil {
		fmt.Fprintf(os.Stderr, "error loading url_groups.yml: %v\n", err)
		os.Exit(1)
	}

	outFile, err := os.Create(*outPath)
	if err != nil {
		fmt.Fprintf(os.Stderr, "error creating output file: %v\n", err)
		os.Exit(1)
	}
	defer outFile.Close()

	bw := bufio.NewWriterSize(outFile, 4<<20) // 4 MB write buffer

	t0 := time.Now()
	nRows, nBad, nNoLocale := processFiles(*date, files, botRules, refererRules, urlCfg, bw)

	if err := bw.Flush(); err != nil {
		fmt.Fprintf(os.Stderr, "error flushing output: %v\n", err)
		os.Exit(1)
	}

	elapsed := time.Since(t0).Seconds()
	rate := 0
	if elapsed > 0 {
		rate = int(float64(nRows) / elapsed)
	}

	// Stats → stderr (visible in rebuild output).
	fmt.Fprintf(os.Stderr,
		"[DATE %s] parse complete: %s rows, %d bad lines, %s no-locale, %.1fs (%s rows/s)\n",
		*date, commaSep(nRows), nBad, commaSep(nNoLocale), elapsed, commaSep(rate))

	// Row count → stdout so the Python caller can read it without parsing stderr.
	fmt.Println(nRows)
}
