#!/usr/bin/env python3
"""
Referer classification module.

Classifies raw referer strings into categories:
  - Direct: empty or "-"
  - Internal: same domain as the site
  - Search Engine: known search engine domains
  - Social: known social media domains
  - AI Platform: known AI chat/assistant domains
  - Other: everything else

Domain matching uses configurable domain lists stored as module-level constants.
"""

from typing import Optional, Tuple
from urllib.parse import urlparse


# ---------------------------------------------------------------------------
# Domain classification lists
# ---------------------------------------------------------------------------

SEARCH_ENGINE_DOMAINS: set = {
    "google.com", "www.google.com",
    "bing.com", "www.bing.com",
    "yandex.com", "www.yandex.com", "yandex.ru", "www.yandex.ru",
    "baidu.com", "www.baidu.com",
    "duckduckgo.com", "www.duckduckgo.com",
    "search.yahoo.com", "yahoo.com", "www.yahoo.com",
    "ecosia.org", "www.ecosia.org",
    "search.brave.com",
    "sogou.com", "www.sogou.com",
    "so.com", "www.so.com",
    "naver.com", "search.naver.com",
}

SOCIAL_DOMAINS: set = {
    "facebook.com", "www.facebook.com", "m.facebook.com",
    "twitter.com", "www.twitter.com", "mobile.twitter.com",
    "x.com", "www.x.com",
    "t.co",
    "linkedin.com", "www.linkedin.com",
    "reddit.com", "www.reddit.com", "old.reddit.com",
    "pinterest.com", "www.pinterest.com",
    "instagram.com", "www.instagram.com",
    "tiktok.com", "www.tiktok.com",
    "youtube.com", "www.youtube.com",
    "vk.com", "www.vk.com",
    "tumblr.com", "www.tumblr.com",
    "threads.net", "www.threads.net",
    "bsky.app",
}

AI_PLATFORM_DOMAINS: set = {
    "chat.openai.com",
    "chatgpt.com", "www.chatgpt.com",
    "perplexity.ai", "www.perplexity.ai",
    "claude.ai", "www.claude.ai",
    "you.com", "www.you.com",
    "bard.google.com",
    "gemini.google.com",
    "copilot.microsoft.com",
    "poe.com", "www.poe.com",
    "phind.com", "www.phind.com",
}

# Pre-built lookup: domain -> referer_type (for fast classification)
_DOMAIN_TYPE_MAP: dict = {}
for _d in SEARCH_ENGINE_DOMAINS:
    _DOMAIN_TYPE_MAP[_d] = "Search Engine"
for _d in SOCIAL_DOMAINS:
    _DOMAIN_TYPE_MAP[_d] = "Social"
for _d in AI_PLATFORM_DOMAINS:
    _DOMAIN_TYPE_MAP[_d] = "AI Platform"


def classify_referer(
    raw_referer: Optional[str],
    site_domain: str,
) -> Tuple[str, Optional[str]]:
    """Classify a raw referer string.

    Args:
        raw_referer: The raw referer header value (may be None, empty, or "-").
        site_domain: The site's own domain (e.g. "example.com") for internal
                     detection. Should be lowercase, without scheme or port.

    Returns:
        (referer_type, referer_path) where:
          - referer_type is one of: "Direct", "Internal", "Search Engine",
            "Social", "AI Platform", "Other"
          - referer_path is the path portion of the referer URL, populated
            only when referer_type is "Internal"; None otherwise.
    """
    if not raw_referer or raw_referer == "-":
        return "Direct", None

    try:
        parsed = urlparse(raw_referer)
    except Exception:
        return "Other", None

    hostname = (parsed.hostname or "").lower()
    if not hostname:
        return "Other", None

    # Internal: same domain (exact match or subdomain of site_domain)
    site_domain_lower = site_domain.lower().strip()
    if site_domain_lower and _is_same_or_subdomain(hostname, site_domain_lower):
        ref_path = parsed.path or "/"
        return "Internal", ref_path

    # Known domain lookup
    ref_type = _DOMAIN_TYPE_MAP.get(hostname)
    if ref_type:
        return ref_type, None

    # Check if hostname is a subdomain of a known domain
    # (e.g., "news.google.com" → Search Engine)
    ref_type = _match_parent_domain(hostname)
    if ref_type:
        return ref_type, None

    return "Other", None


def _is_same_or_subdomain(hostname: str, site_domain: str) -> bool:
    """Check if hostname matches site_domain or is a subdomain of it."""
    if hostname == site_domain:
        return True
    return hostname.endswith("." + site_domain)


def _match_parent_domain(hostname: str) -> Optional[str]:
    """Check if hostname is a subdomain of any known domain."""
    parts = hostname.split(".")
    # Try progressively shorter domain suffixes
    for i in range(1, len(parts)):
        parent = ".".join(parts[i:])
        if parent in _DOMAIN_TYPE_MAP:
            return _DOMAIN_TYPE_MAP[parent]
    return None
