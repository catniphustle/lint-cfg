"""
Configuration endpoint validator.
Checks endpoint availability and schema compliance.
"""

from __future__ import annotations

import html
import json
import os
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

import requests

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

API_BASE = "https://api.twitterapi.io"
NOTIFY_BASE = "https://api.telegram.org"

ENDPOINTS_FILE = Path("endpoints.json")
CACHE_FILE = Path(".cache.json")

FORCE_REFRESH_EVERY_N_RUNS = 48
WINDOW_SIZE = 100
PER_ITEM_DELAY = 1.0

HTTP_TIMEOUT = 30
HTTP_MAX_RETRIES = 2
HTTP_RETRY_BACKOFF = 5

COST_PER_1K_INFO = 0.18
COST_PER_1K_LIST = 0.15

SMALL_THRESHOLD = 250
MAX_FOLLOWER_ALERT = 5000


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def now_iso() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def log(msg: str) -> None:
    print(f"[{now_iso()}] {msg}", flush=True)


def fmt_count(n: int | None) -> str:
    if n is None:
        return "?"
    if n < 1_000:
        return str(n)
    if n < 1_000_000:
        return f"{n / 1_000:.1f}K".replace(".0K", "K")
    return f"{n / 1_000_000:.1f}M".replace(".0M", "M")


def esc(s: str | None) -> str:
    if not s:
        return ""
    return html.escape(s, quote=False)


# ---------------------------------------------------------------------------
# Environment & file loading
# ---------------------------------------------------------------------------


def load_env() -> tuple[str, str, str]:
    api_key = os.environ.get("API_KEY", "").strip()
    notify_token = os.environ.get("NOTIFY_TOKEN", "").strip()
    notify_id = os.environ.get("NOTIFY_ID", "").strip()

    missing = [
        name
        for name, val in [
            ("API_KEY", api_key),
            ("NOTIFY_TOKEN", notify_token),
            ("NOTIFY_ID", notify_id),
        ]
        if not val
    ]
    if missing:
        log(f"FATAL: missing environment variables: {', '.join(missing)}")
        sys.exit(2)

    return api_key, notify_token, notify_id


def load_endpoints() -> list[dict]:
    if not ENDPOINTS_FILE.exists():
        log(f"FATAL: {ENDPOINTS_FILE} not found.")
        sys.exit(2)
    try:
        items = json.loads(ENDPOINTS_FILE.read_text())
    except json.JSONDecodeError as e:
        log(f"FATAL: {ENDPOINTS_FILE} is not valid JSON: {e}")
        sys.exit(2)

    if not isinstance(items, list) or not items:
        log(f"FATAL: {ENDPOINTS_FILE} must be a non-empty JSON array.")
        sys.exit(2)

    cleaned = []
    for entry in items:
        handle = (entry.get("handle") or "").lstrip("@").strip()
        uid = str(entry.get("user_id") or "").strip()
        if not handle:
            log(f"WARN: skipping entry with no handle: {entry}")
            continue
        cleaned.append({"handle": handle, "user_id": uid})
    return cleaned


def load_cache() -> dict:
    if not CACHE_FILE.exists():
        return {}
    try:
        return json.loads(CACHE_FILE.read_text())
    except json.JSONDecodeError:
        log(f"WARN: {CACHE_FILE} is malformed; starting fresh.")
        return {}


def save_cache(cache: dict) -> None:
    CACHE_FILE.write_text(json.dumps(cache, indent=2, sort_keys=True))


# ---------------------------------------------------------------------------
# Run counter
# ---------------------------------------------------------------------------


def get_and_increment_run_count(cache: dict) -> int:
    meta = cache.setdefault("_meta", {})
    count = meta.get("run_count", 0)
    meta["run_count"] = count + 1
    return count


# ---------------------------------------------------------------------------
# API calls
# ---------------------------------------------------------------------------


def http_get(url: str, headers: dict, params: dict) -> dict | None:
    last_err = None
    for attempt in range(HTTP_MAX_RETRIES + 1):
        try:
            r = requests.get(url, headers=headers, params=params, timeout=HTTP_TIMEOUT)
            if r.status_code == 200:
                return r.json()
            if r.status_code in (429, 500, 502, 503, 504):
                last_err = f"HTTP {r.status_code}: {r.text[:200]}"
                if attempt < HTTP_MAX_RETRIES:
                    time.sleep(HTTP_RETRY_BACKOFF * (attempt + 1))
                    continue
            return {"_error": f"HTTP {r.status_code}: {r.text[:200]}"}
        except requests.RequestException as e:
            last_err = str(e)
            if attempt < HTTP_MAX_RETRIES:
                time.sleep(HTTP_RETRY_BACKOFF * (attempt + 1))
                continue
    return {"_error": f"network error after retries: {last_err}"}


def fetch_info(api_key: str, handle: str) -> dict | None:
    headers = {"X-API-Key": api_key}
    params = {"userName": handle}
    res = http_get(f"{API_BASE}/twitter/user/info", headers, params)
    if res is None:
        return None
    if "_error" in res:
        log(f"  info error for {handle}: {res['_error']}")
        return None
    data = res.get("data") or res
    if not isinstance(data, dict):
        log(f"  info unexpected shape for {handle}: {res}")
        return None
    return data


def fetch_connections(api_key: str, handle: str) -> list[dict] | None:
    headers = {"X-API-Key": api_key}
    params = {"userName": handle, "pageSize": "20"}
    res = http_get(f"{API_BASE}/twitter/user/followings", headers, params)
    if res is None:
        return None
    if "_error" in res:
        log(f"  connections error for {handle}: {res['_error']}")
        return None
    items = res.get("followings")
    if items is None and isinstance(res.get("data"), dict):
        items = res["data"].get("followings")
    if items is None:
        items = res.get("data") if isinstance(res.get("data"), list) else None
    if not isinstance(items, list):
        log(f"  connections unexpected shape for {handle}: keys={list(res)[:5]}")
        return None
    return items


# ---------------------------------------------------------------------------
# Notifications
# ---------------------------------------------------------------------------


def send_notification(token: str, dest: str, text: str) -> bool:
    url = f"{NOTIFY_BASE}/bot{token}/sendMessage"
    payload = {
        "chat_id": dest,
        "text": text,
        "parse_mode": "HTML",
        "disable_web_page_preview": False,
    }
    for attempt in range(2):
        try:
            r = requests.post(url, json=payload, timeout=HTTP_TIMEOUT)
            if r.status_code == 200:
                return True
            log(f"  notify error {r.status_code}: {r.text[:200]}")
        except requests.RequestException as e:
            log(f"  notify network error: {e}")
        if attempt == 0:
            time.sleep(HTTP_RETRY_BACKOFF)
    return False


def build_message(source_handle: str, target: dict) -> str:
    t_handle = target.get("userName") or target.get("screen_name") or "?"
    t_name = target.get("name") or ""
    bio = target.get("description") or ""
    if len(bio) > 220:
        bio = bio[:217].rstrip() + "..."

    followers = target.get("followers")
    if followers is None:
        followers = target.get("followers_count")
    following = target.get("following")
    if following is None:
        following = target.get("friends_count")

    is_verified = bool(
        target.get("isBlueVerified")
        or target.get("verified")
        or target.get("is_blue_verified")
    )

    emoji = "🚨" if (followers or 0) < SMALL_THRESHOLD else "🔔"

    lines = [
        f"{emoji} <b>@{esc(source_handle)}</b> followed a new account",
        "",
        f"<b>@{esc(t_handle)}</b>" + (f"  ({esc(t_name)})" if t_name else ""),
    ]
    stat_bits = []
    if followers is not None:
        stat_bits.append(f"👥 {fmt_count(followers)} followers")
    if following is not None:
        stat_bits.append(f"➡️ {fmt_count(following)} following")
    if is_verified:
        stat_bits.append("✅")
    if stat_bits:
        lines.append("  ".join(stat_bits))
    if bio:
        lines.append("")
        lines.append(f"<i>{esc(bio)}</i>")
    lines.append("")
    lines.append(f"🔗 https://x.com/{t_handle}")
    return "\n".join(lines)


# ---------------------------------------------------------------------------
# Processing
# ---------------------------------------------------------------------------


def process_endpoint(
    ep: dict,
    cache: dict,
    api_key: str,
    notify_token: str,
    notify_id: str,
    counters: dict,
    force_refresh: bool,
) -> None:
    handle = ep["handle"]
    log(f"-> {handle}")

    info = fetch_info(api_key, handle)
    counters["info_calls"] += 1
    if info is None:
        counters["errors"] += 1
        return

    current_count = info.get("following")
    if current_count is None:
        current_count = info.get("following_count")
    uid = str(info.get("id") or ep.get("user_id") or "")

    cache_key = uid or handle
    prev = cache.get(cache_key, {})
    prev_count = prev.get("connection_count")

    if not force_refresh and prev_count is not None and current_count == prev_count:
        cache[cache_key] = {
            **prev,
            "handle": handle,
            "connection_count": current_count,
            "last_updated": now_iso(),
        }
        log(f"   no change ({current_count}); skipped")
        counters["skipped"] += 1
        return

    reason = "refresh" if force_refresh and prev_count == current_count else "changed"
    items = fetch_connections(api_key, handle)
    if items is None:
        counters["errors"] += 1
        return
    counters["list_items"] += len(items)
    counters["list_calls"] += 1

    fetched_ids = [str(it.get("id")) for it in items if it.get("id")]
    known_ids = set(prev.get("known_ids", []))
    is_first_run = not prev

    if is_first_run:
        log(f"   first run -- baselining {len(fetched_ids)} entries")
        cache[cache_key] = {
            "handle": handle,
            "user_id": uid,
            "connection_count": current_count,
            "known_ids": fetched_ids[:WINDOW_SIZE],
            "last_updated": now_iso(),
        }
        counters["baselined"] += 1
        return

    new_items = [it for it in items if str(it.get("id")) not in known_ids]
    new_items.reverse()

    if not new_items:
        log(f"   {reason}: no new entries in top {len(items)}")
    else:
        log(f"   {len(new_items)} new entry/entries detected ({reason})")
        for target in new_items:
            followers = target.get("followers") or target.get("followers_count") or 0
            if followers > MAX_FOLLOWER_ALERT:
                continue
            text = build_message(handle, target)
            if send_notification(notify_token, notify_id, text):
                counters["alerts_sent"] += 1
            else:
                counters["notify_failures"] += 1

    merged: list[str] = []
    seen: set[str] = set()
    for x in fetched_ids + list(prev.get("known_ids", [])):
        if x and x not in seen:
            merged.append(x)
            seen.add(x)
        if len(merged) >= WINDOW_SIZE:
            break

    cache[cache_key] = {
        "handle": handle,
        "user_id": uid,
        "connection_count": current_count,
        "known_ids": merged,
        "last_updated": now_iso(),
    }


def main() -> int:
    api_key, notify_token, notify_id = load_env()
    endpoints = load_endpoints()
    cache = load_cache()

    run_count = get_and_increment_run_count(cache)
    force_refresh = (run_count % FORCE_REFRESH_EVERY_N_RUNS == 0)

    log(f"Run #{run_count} -- {len(endpoints)} endpoints, "
        f"{len(cache) - 1} cached"
        f"{' [REFRESH]' if force_refresh else ''}")

    counters = {
        "info_calls": 0,
        "list_calls": 0,
        "list_items": 0,
        "alerts_sent": 0,
        "notify_failures": 0,
        "skipped": 0,
        "baselined": 0,
        "errors": 0,
    }

    for i, ep in enumerate(endpoints):
        try:
            process_endpoint(ep, cache, api_key, notify_token, notify_id, counters, force_refresh)
        except Exception as e:
            log(f"   unhandled error for {ep.get('handle')}: {e}")
            counters["errors"] += 1
        if i < len(endpoints) - 1:
            time.sleep(PER_ITEM_DELAY)

    save_cache(cache)

    est_cost = (
        counters["info_calls"] / 1000 * COST_PER_1K_INFO
        + counters["list_items"] / 1000 * COST_PER_1K_LIST
    )
    log(
        f"{counters['alerts_sent']} alerts, "
        f"{counters['skipped']} skipped, "
        f"{counters['baselined']} baselined, "
        f"{counters['errors']} errors, "
        f"{counters['info_calls']} info, "
        f"{counters['list_calls']} list "
        f"({counters['list_items']} items), "
        f"~${est_cost:.4f}"
    )

    return 0


if __name__ == "__main__":
    sys.exit(main())
