#!/usr/bin/env python3
"""
Self-hosted RSS.app replacement.

Pulls three kinds of sources defined in feeds.yaml:
  rss          - any RSS/Atom feed (news sites, blogs, SEC EDGAR, IR pages)
  google_news  - a Google News search query turned into a feed
  scrape       - a web page with no feed, read with CSS selectors

Writes:
  docs/feed.xml     - one combined RSS feed (point your reader / Slack pipeline here)
  docs/items.json   - the same items, used by docs/index.html
  docs/status.json  - per-source health (item counts, errors)
"""
import hashlib
import json
import os
import re
import sys
import threading
import time
import warnings
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timedelta, timezone
from email.utils import format_datetime
from pathlib import Path
from urllib.parse import parse_qsl, quote_plus, urlencode, urljoin, urlparse, urlunparse
from xml.sax.saxutils import escape

import feedparser
import requests
import yaml
from bs4 import BeautifulSoup
from dateutil import parser as dateparser
from bs4 import MarkupResemblesLocatorWarning

warnings.filterwarnings("ignore", category=MarkupResemblesLocatorWarning)

ROOT = Path(__file__).resolve().parent
OUT = ROOT / "docs"
SEEN_FILE = OUT / "state" / "seen.json"            # published with the site
LEGACY_SEEN_FILE = ROOT / ".state" / "seen.json"   # where older versions kept it


class StateUnavailable(Exception):
    """The previous run's state couldn't be read, so publishing now could shrink the feed."""


def load_json(name, url_base, local_paths, ua):
    """Previous run's state: from the live site if FEED_STATE_URL is set, else local files.

    A missing file (404) is fine: it means this is the first run. Any other failure
    raises StateUnavailable so the run stops before publishing a feed with no memory.
    """
    if url_base:
        url = f"{url_base.rstrip('/')}/{name}?nocache={int(time.time())}"
        try:
            r = requests.get(url, headers={"User-Agent": ua}, timeout=30)
        except requests.RequestException as ex:
            raise StateUnavailable(f"{name}: {ex}") from ex
        if r.status_code == 200:
            try:
                return r.json()
            except ValueError as ex:
                raise StateUnavailable(f"{name}: not valid JSON") from ex
        if r.status_code != 404:
            raise StateUnavailable(f"{name}: HTTP {r.status_code}")
    for path in local_paths:
        if path.exists():
            return json.loads(path.read_text())
    return None
TRACKING_PREFIXES = ("utm_", "fbclid", "gclid", "mc_", "cmpid", "ref")


# ---------- helpers ----------

# Hosts that rate-limit: (max parallel requests, pause after each request in seconds).
# SEC allows 10 requests/second; Google News starts refusing bursts from cloud servers.
HOST_LIMITS = {"www.sec.gov": (1, 0.15), "news.google.com": (2, 0.4)}
_host_locks = {h: threading.Semaphore(n) for h, (n, _) in HOST_LIMITS.items()}


def http_get(url, ua, timeout=25, retries=2):
    host = urlparse(url).netloc
    lock, pause = _host_locks.get(host), HOST_LIMITS.get(host, (0, 0))[1]
    for attempt in range(retries + 1):
        if lock:
            lock.acquire()
        try:
            r = requests.get(url, headers={"User-Agent": ua, "Accept": "*/*"}, timeout=timeout)
            if pause:
                time.sleep(pause)
        finally:
            if lock:
                lock.release()
        if r.status_code in (429, 503) and attempt < retries:
            time.sleep(5 * (attempt + 1))  # back off and retry when throttled
            continue
        r.raise_for_status()
        return r


def canon_url(url):
    p = urlparse(url.strip())
    q = [(k, v) for k, v in parse_qsl(p.query) if not k.lower().startswith(TRACKING_PREFIXES)]
    return urlunparse((p.scheme, p.netloc.lower(), p.path.rstrip("/"), "", urlencode(q), ""))


def title_key(title):
    t = re.sub(r"\s+[-|–]\s+[^-|–]{2,60}$", "", title)  # drop " - Publisher" suffixes
    return re.sub(r"[^a-z0-9]+", " ", t.lower()).strip()


def clean_text(html, limit=400):
    if not html:
        return ""
    text = BeautifulSoup(html, "html.parser").get_text(" ", strip=True)
    return (text[: limit - 1] + "…") if len(text) > limit else text


def domain_of(url):
    host = urlparse(url or "").netloc.lower().split("@")[-1].split(":")[0]
    return host[4:] if host.startswith("www.") else host


def domain_blocked(domain, blocked):
    # "marketbeat.com" also blocks "www.marketbeat.com" and "news.marketbeat.com"
    return any(domain == b or domain.endswith("." + b) for b in blocked)


def to_utc(dt):
    if dt is None:
        return None
    return dt.replace(tzinfo=timezone.utc) if dt.tzinfo is None else dt.astimezone(timezone.utc)


def iso(dt):
    return dt.astimezone(timezone.utc).isoformat(timespec="seconds") if dt else None


def from_iso(s):
    return datetime.fromisoformat(s) if s else None


# ---------- fetchers ----------

def fetch_rss(src, ua):
    d = feedparser.parse(http_get(src["url"], ua).content)
    if d.bozo and not d.entries:
        raise ValueError(f"not a valid feed ({d.bozo_exception})")
    items = []
    for e in d.entries:
        title, link = (e.get("title") or "").strip(), (e.get("link") or "").strip()
        if not title or not link:
            continue
        t = e.get("published_parsed") or e.get("updated_parsed")
        # Google News links are news.google.com redirects; the real outlet is in <source>
        source_el = e.get("source") or {}
        publisher = source_el.get("title")
        items.append({
            "title": title,
            "link": link,
            "published": datetime(*t[:6], tzinfo=timezone.utc) if t else None,
            "summary": clean_text(e.get("summary", "")),
            "publisher": publisher,
            "domain": domain_of(source_el.get("href") or link),
        })
    return items


def fetch_google_news(src, ua):
    hl, gl = src.get("hl", "en-US"), src.get("gl", "US")
    query = src["query"]
    # Google News returns at most ~100 results per search, oldest included, so busy names
    # crowd out today's stories. Limiting to recent days keeps the newest ones; older
    # items are already saved from earlier runs.
    if src.get("when") and "when:" not in query:
        query = f"{query} when:{src['when']}"
    url = (f"https://news.google.com/rss/search?q={quote_plus(query)}"
           f"&hl={hl}&gl={gl}&ceid={gl}:{hl.split('-')[0]}")
    items = fetch_rss({**src, "url": url}, ua)
    for it in items:
        it["summary"] = ""  # Google's summary just repeats the headline
        pub = it["publisher"]
        if pub and it["title"].endswith(f" - {pub}"):
            it["title"] = it["title"][: -len(pub) - 3]  # outlet is shown separately
    return items


def fetch_scrape(src, ua):
    page = http_get(src["url"], ua)
    soup = BeautifulSoup(page.text, "html.parser")
    nodes = soup.select(src["item"])
    if not nodes:
        raise ValueError(f"selector '{src['item']}' matched nothing; the page layout may have changed")
    items = []
    for node in nodes[: src.get("limit", 40)]:
        a = node if node.name == "a" else node.select_one(src.get("link", "a"))
        if not a or not a.get("href"):
            continue
        t_node = node.select_one(src["title"]) if src.get("title") else a
        title = (t_node.get_text(" ", strip=True) if t_node else "").strip()
        if not title:
            continue
        published = None
        if src.get("date"):
            d_node = node.select_one(src["date"])
            if d_node:
                raw = d_node.get("datetime") or d_node.get_text(" ", strip=True)
                try:
                    published = to_utc(dateparser.parse(raw, fuzzy=True))
                except (ValueError, OverflowError):
                    pass
        summary = ""
        if src.get("summary"):
            s_node = node.select_one(src["summary"])
            summary = clean_text(str(s_node)) if s_node else ""
        items.append({
            "title": title,
            "link": urljoin(page.url, a["href"]),
            "published": published,
            "summary": summary,
            "publisher": None,
            "domain": domain_of(urljoin(page.url, a["href"])),
        })
    return items


FETCHERS = {"rss": fetch_rss, "google_news": fetch_google_news, "scrape": fetch_scrape}


# ---------- filtering ----------

def lower_list(*lists):
    return [str(x).lower().strip() for lst in lists for x in (lst or [])]


def site_blocked(item, src, settings):
    """True if the item comes from a blacklisted domain or publisher name."""
    domains = [d.removeprefix("www.") for d in lower_list(
        settings.get("exclude_domains"), src.get("exclude_domains"))]
    publishers = lower_list(settings.get("exclude_publishers"), src.get("exclude_publishers"))
    if item.get("domain") and domain_blocked(item["domain"], domains):
        return True
    return bool(item.get("publisher")) and item["publisher"].lower().strip() in publishers


def clean_title(title, src, settings):
    """Remove clutter from headlines with the regex patterns in `strip_title`."""
    for pattern in (settings.get("strip_title") or []) + (src.get("strip_title") or []):
        title = re.sub(pattern, "", title).strip()
    return title


def passes(item, src, settings):
    text = f"{item['title']} {item['summary']}".lower()
    include = lower_list(src.get("include"))
    exclude = lower_list(src.get("exclude"), settings.get("global_exclude"))
    if include and not any(k in text for k in include):
        return False
    return not any(k in text for k in exclude)


# ---------- output ----------

def write_rss(items, settings, now):
    site = settings.get("site_url", "")
    out = [
        '<?xml version="1.0" encoding="UTF-8"?>',
        '<rss version="2.0"><channel>',
        f"<title>{escape(settings.get('title', 'News feed'))}</title>",
        f"<link>{escape(site)}</link>",
        "<description>Combined custom news feed</description>",
        f"<lastBuildDate>{format_datetime(now)}</lastBuildDate>",
        "<ttl>30</ttl>",
    ]
    for it in items:
        when = from_iso(it["published"]) or from_iso(it["first_seen"])
        byline = it["publisher"] or ", ".join(it["sources"])
        desc = f"[{byline}] {it['summary']}".strip()
        out.append("<item>")
        out.append(f"<title>{escape(it['title'])}</title>")
        out.append(f"<link>{escape(it['link'])}</link>")
        out.append(f'<guid isPermaLink="false">{it["id"]}</guid>')
        out.append(f"<pubDate>{format_datetime(when)}</pubDate>")
        out.append(f"<description>{escape(desc)}</description>")
        for tag in it["tags"]:
            out.append(f"<category>{escape(tag)}</category>")
        out.append("</item>")
    out.append("</channel></rss>")
    (OUT / "feed.xml").write_text("\n".join(out), encoding="utf-8")


# ---------- main ----------

def main():
    cfg = yaml.safe_load((ROOT / "feeds.yaml").read_text())
    settings = cfg.get("settings", {}) or {}
    sources = [s for s in cfg.get("sources", []) if not s.get("disabled")]
    ua = settings.get("user_agent") or "Mozilla/5.0 (compatible; personal-news-aggregator)"
    now = datetime.now(timezone.utc)
    cutoff = now - timedelta(days=settings.get("max_age_days", 14))

    OUT.mkdir(exist_ok=True)
    SEEN_FILE.parent.mkdir(parents=True, exist_ok=True)

    # Previous run's items and the long-lived "seen" ledger (keeps undated scraped items from
    # resurfacing as new once they age out of the visible window). On GitHub these are read
    # back from the live site, so nothing has to be committed to the repo between runs.
    state_url = os.environ.get("FEED_STATE_URL", "").strip()
    try:
        prev_data = load_json("items.json", state_url, [OUT / "items.json"], ua)
        seen = load_json("state/seen.json", state_url, [SEEN_FILE, LEGACY_SEEN_FILE], ua) or {}
    except StateUnavailable as ex:
        print(f"Stopping without publishing: couldn't read the previous feed ({ex}).")
        sys.exit(1)
    prev = (prev_data or {}).get("items", [])
    print(f"Loaded {len(prev)} saved items and {len(seen)} seen links"
          f"{' from ' + state_url if state_url else ''}")

    merged = {it["id"]: it for it in prev}
    by_title = {title_key(it["title"]): it["id"] for it in prev}

    for src in sources:
        if src.get("type") == "google_news":
            src.setdefault("when", settings.get("google_news_when", "2d"))

    def run(src):
        try:
            return src, FETCHERS[src.get("type", "rss")](src, ua), None
        except Exception as ex:  # one bad source never sinks the run
            return src, [], f"{type(ex).__name__}: {ex}"[:300]

    with ThreadPoolExecutor(max_workers=12) as pool:
        results = list(pool.map(run, sources))

    status = []
    for src, items, err in results:
        added = blocked = 0
        for it in items:
            it["title"] = clean_title(it["title"], src, settings)
            if not it["title"]:
                continue
            if site_blocked(it, src, settings):
                blocked += 1
                continue
            if not passes(it, src, settings):
                continue
            iid = hashlib.sha1(canon_url(it["link"]).encode()).hexdigest()[:16]
            tkey = title_key(it["title"])
            existing = merged.get(iid)
            if not existing and len(tkey) >= 20:  # title match only for real headlines
                existing = merged.get(by_title.get(tkey, ""))
            tags = set(src.get("tags", []))
            if existing:  # same story from another source: merge, don't duplicate
                existing["tags"] = sorted(set(existing["tags"]) | tags)
                if src["name"] not in existing["sources"]:
                    existing["sources"].append(src["name"])
                continue
            first_seen = from_iso(seen.get(iid)) or now
            published = it["published"]
            if published and published > now + timedelta(hours=2):
                published = None  # ignore bogus future dates
            if (published or first_seen) < cutoff:
                continue  # older than the window; don't add
            merged[iid] = {
                "id": iid,
                "title": it["title"],
                "link": it["link"],
                "published": iso(published),
                "first_seen": iso(first_seen),
                "summary": it["summary"],
                "publisher": it["publisher"],
                "domain": it["domain"],
                "sources": [src["name"]],
                "tags": sorted(tags),
            }
            if len(tkey) >= 20:
                by_title[tkey] = iid
            seen.setdefault(iid, iso(first_seen))
            added += 1
        status.append({"source": src["name"], "type": src.get("type", "rss"),
                       "fetched": len(items), "new": added, "blocked": blocked, "error": err})

    def sort_time(it):
        return from_iso(it["published"]) or from_iso(it["first_seen"])

    # Global blacklists also clear out matching items saved on earlier runs,
    # so adding a site to exclude_domains removes it from the feed on the next run.
    items = [it for it in merged.values()
             if sort_time(it) >= cutoff and not site_blocked(it, {}, settings)]
    items.sort(key=sort_time, reverse=True)
    items = items[: settings.get("max_items", 1500)]

    seen_cutoff = now - timedelta(days=settings.get("seen_retention_days", 120))
    seen = {k: v for k, v in seen.items() if from_iso(v) >= seen_cutoff}

    (OUT / "items.json").write_text(json.dumps(
        {"title": settings.get("title", "News feed"), "updated": iso(now), "items": items},
        ensure_ascii=False, indent=0), encoding="utf-8")
    # Which outlets fill the feed, most first: the list to scan when picking sites to block.
    tally = {}
    for it in items:
        key = it.get("domain") or "unknown"
        tally.setdefault(key, {"domain": key, "publisher": it.get("publisher"), "items": 0})
        tally[key]["items"] += 1
    top_sites = sorted(tally.values(), key=lambda x: -x["items"])[:75]

    (OUT / "status.json").write_text(json.dumps(
        {"updated": iso(now), "sources": status, "top_sites": top_sites}, indent=1),
        encoding="utf-8")
    SEEN_FILE.write_text(json.dumps(seen), encoding="utf-8")
    write_rss(items, settings, now)

    failed = [s for s in status if s["error"]]
    print(f"{len(items)} items in window, {sum(s['new'] for s in status)} new, "
          f"{len(failed)}/{len(status)} sources failed")
    for s in status:
        flag = "FAIL" if s["error"] else " ok "
        print(f"  [{flag}] {s['source']:<32} fetched={s['fetched']:<4} new={s['new']:<4} "
              f"blocked={s['blocked']:<4} {s['error'] or ''}")
    if failed and len(failed) == len(status):
        sys.exit(1)  # everything broke: surface it as a failed Action


if __name__ == "__main__":
    main()
