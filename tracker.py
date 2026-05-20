"""
KOL Follow Tracker
==================
Polls twitterapi.io every 15 minutes for new follows by a list of X (Twitter)
KOLs and posts new follows to a Telegram chat.

Uses a "following_count" pre-check to skip expensive API calls when nothing
has changed. Every 12th run (~3 hours), forces a full followings fetch
regardless of count, to catch the edge case where a KOL follows someone
AND unfollows someone else in the same window (net count unchanged).

Designed to run as a GitHub Actions cron job. Reads secrets from environment
variables, reads the KOL list from kols.json, persists state to state.json
(which the workflow commits back to the repo each run).

Environment variables required:
    TWITTERAPI_IO_KEY    - API key from twitterapi.io
    TELEGRAM_BOT_TOKEN   - Bot token from @BotFather
    TELEGRAM_CHAT_ID     - Destination chat ID (use @userinfobot to find yours)

Files:
    kols.json    - list of {"handle": "...", "user_id": "..."} (you create this)
    state.json   - script-managed; created on first run
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
TELEGRAM_BASE = "https://api.telegram.org"

# Files (relative to repo root)
KOLS_FILE = Path("kols.json")
STATE_FILE = Path("state.json")

# Every Nth run, skip the pre-check and fetch followings for ALL KOLs
# regardless of whether their following_count changed. This catches the edge
# case where a KOL follows + unfollows in the same window (net count = 0).
# At 15-minute polling intervals, 12 runs = every 3 hours.
FORCE_FETCH_EVERY_N_RUNS = 12

# How many of each KOL's most-recent follows we remember between runs.
KNOWN_WINDOW_SIZE = 100

# Politeness delay between KOLs (seconds)
PER_KOL_DELAY = 1.0

# HTTP timeouts and retries
HTTP_TIMEOUT = 30
HTTP_MAX_RETRIES = 2
HTTP_RETRY_BACKOFF = 5  # seconds

# Pricing constants for the run-summary log line (prices per 1,000 items)
COST_PER_1K_USERINFO = 0.18
COST_PER_1K_FOLLOWINGS = 0.15


# ---------------------------------------------------------------------------
# Small helpers
# ---------------------------------------------------------------------------


def now_iso() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def log(msg: str) -> None:
    print(f"[{now_iso()}] {msg}", flush=True)


def fmt_count(n: int | None) -> str:
    """Format follower counts: 12345 -> '12.3K', 1234567 -> '1.2M'."""
    if n is None:
        return "?"
    if n < 1_000:
        return str(n)
    if n < 1_000_000:
        return f"{n / 1_000:.1f}K".replace(".0K", "K")
    return f"{n / 1_000_000:.1f}M".replace(".0M", "M")


def esc(s: str | None) -> str:
    """HTML-escape user-supplied strings for Telegram HTML parse mode."""
    if not s:
        return ""
    return html.escape(s, quote=False)


# ---------------------------------------------------------------------------
# Environment & file loading
# ---------------------------------------------------------------------------


def load_env() -> tuple[str, str, str]:
    api_key = os.environ.get("TWITTERAPI_IO_KEY", "").strip()
    bot_token = os.environ.get("TELEGRAM_BOT_TOKEN", "").strip()
    chat_id = os.environ.get("TELEGRAM_CHAT_ID", "").strip()

    missing = [
        name
        for name, val in [
            ("TWITTERAPI_IO_KEY", api_key),
            ("TELEGRAM_BOT_TOKEN", bot_token),
            ("TELEGRAM_CHAT_ID", chat_id),
        ]
        if not val
    ]
    if missing:
        log(f"FATAL: missing environment variables: {', '.join(missing)}")
        sys.exit(2)

    return api_key, bot_token, chat_id


def load_kols() -> list[dict]:
    if not KOLS_FILE.exists():
        log(f"FATAL: {KOLS_FILE} not found. Create it with a list of "
            f'[{{"handle": "...", "user_id": "..."}}, ...]')
        sys.exit(2)
    try:
        kols = json.loads(KOLS_FILE.read_text())
    except json.JSONDecodeError as e:
        log(f"FATAL: {KOLS_FILE} is not valid JSON: {e}")
        sys.exit(2)

    if not isinstance(kols, list) or not kols:
        log(f"FATAL: {KOLS_FILE} must be a non-empty JSON array.")
        sys.exit(2)

    cleaned = []
    for entry in kols:
        handle = (entry.get("handle") or "").lstrip("@").strip()
        user_id = str(entry.get("user_id") or "").strip()
        if not handle:
            log(f"WARN: skipping kols.json entry with no handle: {entry}")
            continue
        cleaned.append({"handle": handle, "user_id": user_id})
    return cleaned


def load_state() -> dict:
    if not STATE_FILE.exists():
        return {}
    try:
        return json.loads(STATE_FILE.read_text())
    except json.JSONDecodeError:
        log(f"WARN: {STATE_FILE} is malformed; starting fresh.")
        return {}


def save_state(state: dict) -> None:
    STATE_FILE.write_text(json.dumps(state, indent=2, sort_keys=True))


# ---------------------------------------------------------------------------
# Run counter (persisted in state.json under "_meta")
# ---------------------------------------------------------------------------


def get_and_increment_run_count(state: dict) -> int:
    """Returns the current run number (0-indexed) and increments for next time."""
    meta = state.setdefault("_meta", {})
    count = meta.get("run_count", 0)
    meta["run_count"] = count + 1
    return count


# ---------------------------------------------------------------------------
# twitterapi.io calls
# ---------------------------------------------------------------------------


def http_get(url: str, headers: dict, params: dict) -> dict | None:
    """GET with simple retry. Returns parsed JSON or None on failure."""
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


def fetch_user_info(api_key: str, handle: str) -> dict | None:
    """Returns the user info dict, or None on failure."""
    headers = {"X-API-Key": api_key}
    params = {"userName": handle}
    res = http_get(f"{API_BASE}/twitter/user/info", headers, params)
    if res is None:
        return None
    if "_error" in res:
        log(f"  user/info error for @{handle}: {res['_error']}")
        return None
    data = res.get("data") or res
    if not isinstance(data, dict):
        log(f"  user/info unexpected shape for @{handle}: {res}")
        return None
    return data


def fetch_recent_followings(api_key: str, handle: str) -> list[dict] | None:
    """Returns the FIRST PAGE of followings (most recent first), or None."""
    headers = {"X-API-Key": api_key}
    params = {"userName": handle}
    res = http_get(f"{API_BASE}/twitter/user/followings", headers, params)
    if res is None:
        return None
    if "_error" in res:
        log(f"  user/followings error for @{handle}: {res['_error']}")
        return None
    items = res.get("followings")
    if items is None and isinstance(res.get("data"), dict):
        items = res["data"].get("followings")
    if items is None:
        items = res.get("data") if isinstance(res.get("data"), list) else None
    if not isinstance(items, list):
        log(f"  user/followings unexpected shape for @{handle}: keys={list(res)[:5]}")
        return None
    return items


# ---------------------------------------------------------------------------
# Telegram delivery
# ---------------------------------------------------------------------------


def telegram_send(bot_token: str, chat_id: str, text: str) -> bool:
    url = f"{TELEGRAM_BASE}/bot{bot_token}/sendMessage"
    payload = {
        "chat_id": chat_id,
        "text": text,
        "parse_mode": "HTML",
        "disable_web_page_preview": False,
    }
    for attempt in range(2):
        try:
            r = requests.post(url, json=payload, timeout=HTTP_TIMEOUT)
            if r.status_code == 200:
                return True
            log(f"  telegram error {r.status_code}: {r.text[:200]}")
        except requests.RequestException as e:
            log(f"  telegram network error: {e}")
        if attempt == 0:
            time.sleep(HTTP_RETRY_BACKOFF)
    return False


def build_alert(kol_handle: str, followed: dict) -> str:
    """Format a single Telegram alert for one new follow."""
    new_handle = followed.get("userName") or followed.get("screen_name") or "?"
    new_name = followed.get("name") or ""
    bio = followed.get("description") or ""
    if len(bio) > 220:
        bio = bio[:217].rstrip() + "..."

    followers = followed.get("followers")
    if followers is None:
        followers = followed.get("followers_count")
    following = followed.get("following")
    if following is None:
        following = followed.get("friends_count")

    is_verified = bool(
        followed.get("isBlueVerified")
        or followed.get("verified")
        or followed.get("is_blue_verified")
    )

    lines = [
        f"🔔 <b>@{esc(kol_handle)}</b> followed a new account",
        "",
        f"<b>@{esc(new_handle)}</b>" + (f"  ({esc(new_name)})" if new_name else ""),
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
    lines.append(f"🔗 https://x.com/{new_handle}")
    return "\n".join(lines)


# ---------------------------------------------------------------------------
# Main loop
# ---------------------------------------------------------------------------


def process_kol(
    kol: dict,
    state: dict,
    api_key: str,
    bot_token: str,
    chat_id: str,
    counters: dict,
    force_fetch: bool,
) -> None:
    handle = kol["handle"]
    log(f"-> @{handle}")

    info = fetch_user_info(api_key, handle)
    counters["userinfo_calls"] += 1
    if info is None:
        counters["errors"] += 1
        return

    current_following = info.get("following")
    if current_following is None:
        current_following = info.get("following_count")
    user_id = str(info.get("id") or kol.get("user_id") or "")

    state_key = user_id or handle
    prev = state.get(state_key, {})
    prev_following = prev.get("following_count")

    # ---- Pre-check: skip the followings call if count is unchanged
    #      UNLESS this is a force-fetch run (every 3 hours)
    if not force_fetch and prev_following is not None and current_following == prev_following:
        state[state_key] = {
            **prev,
            "handle": handle,
            "following_count": current_following,
            "last_updated": now_iso(),
        }
        log(f"   no change (following={current_following}); skipped")
        counters["skipped"] += 1
        return

    # ---- Fetch the recent followings page
    reason = "force-fetch" if force_fetch and prev_following == current_following else "count changed"
    items = fetch_recent_followings(api_key, handle)
    if items is None:
        counters["errors"] += 1
        return
    counters["followings_items"] += len(items)
    counters["followings_calls"] += 1

    fetched_ids = [str(it.get("id")) for it in items if it.get("id")]
    known_ids = set(prev.get("known_following_ids", []))
    is_first_run = not prev

    if is_first_run:
        log(f"   first run -- baselining {len(fetched_ids)} known follows, no alerts")
        new_window = fetched_ids[:KNOWN_WINDOW_SIZE]
        state[state_key] = {
            "handle": handle,
            "user_id": user_id,
            "following_count": current_following,
            "known_following_ids": new_window,
            "last_updated": now_iso(),
        }
        counters["baselined"] += 1
        return

    # ---- Diff
    new_follows = [it for it in items if str(it.get("id")) not in known_ids]
    new_follows.reverse()

    if not new_follows:
        log(f"   {reason}: no new IDs in top {len(items)} "
            f"(count {prev_following} -> {current_following})")
    else:
        log(f"   {len(new_follows)} new follow(s) detected ({reason})")
        for followed in new_follows:
            text = build_alert(handle, followed)
            if telegram_send(bot_token, chat_id, text):
                counters["alerts_sent"] += 1
            else:
                counters["telegram_failures"] += 1

    # Merge into rolling window
    merged: list[str] = []
    seen: set[str] = set()
    for x in fetched_ids + list(prev.get("known_following_ids", [])):
        if x and x not in seen:
            merged.append(x)
            seen.add(x)
        if len(merged) >= KNOWN_WINDOW_SIZE:
            break

    state[state_key] = {
        "handle": handle,
        "user_id": user_id,
        "following_count": current_following,
        "known_following_ids": merged,
        "last_updated": now_iso(),
    }


def main() -> int:
    api_key, bot_token, chat_id = load_env()
    kols = load_kols()
    state = load_state()

    run_count = get_and_increment_run_count(state)
    force_fetch = (run_count % FORCE_FETCH_EVERY_N_RUNS == 0)

    log(f"Starting run #{run_count} -- {len(kols)} KOLs, "
        f"{len(state) - 1} with prior state"  # -1 for _meta
        f"{' [FORCE FETCH]' if force_fetch else ''}")

    counters = {
        "userinfo_calls": 0,
        "followings_calls": 0,
        "followings_items": 0,
        "alerts_sent": 0,
        "telegram_failures": 0,
        "skipped": 0,
        "baselined": 0,
        "errors": 0,
    }

    for i, kol in enumerate(kols):
        try:
            process_kol(kol, state, api_key, bot_token, chat_id, counters, force_fetch)
        except Exception as e:
            log(f"   unhandled error for @{kol.get('handle')}: {e}")
            counters["errors"] += 1
        if i < len(kols) - 1:
            time.sleep(PER_KOL_DELAY)

    save_state(state)

    est_cost = (
        counters["userinfo_calls"] / 1000 * COST_PER_1K_USERINFO
        + counters["followings_items"] / 1000 * COST_PER_1K_FOLLOWINGS
    )
    log(
        "Run summary: "
        f"{counters['alerts_sent']} alerts, "
        f"{counters['skipped']} skipped, "
        f"{counters['baselined']} baselined, "
        f"{counters['errors']} errors, "
        f"{counters['userinfo_calls']} info calls, "
        f"{counters['followings_calls']} followings calls "
        f"({counters['followings_items']} items), "
        f"~${est_cost:.4f}"
    )

    return 0


if __name__ == "__main__":
    sys.exit(main())
