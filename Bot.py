# bot.py (Part 1 of 2)
import sys, types; sys.modules['imghdr'] = types.ModuleType('imghdr')

import logging
import sqlite3
import requests
import json
from datetime import datetime, timedelta
from functools import wraps

from telegram import (
    Update,
    InlineKeyboardButton,
    InlineKeyboardMarkup,
    ParseMode,
    ReplyKeyboardMarkup,
    KeyboardButton
)
from telegram.ext import (
    Updater,
    CommandHandler,
    MessageHandler,
    Filters,
    CallbackQueryHandler,
    ChatMemberHandler,
    CallbackContext
)

# =============== LOGGING ===============
logging.basicConfig(
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    level=logging.INFO
)
logger = logging.getLogger(__name__)

# =============== BOT TOKEN ===============
BOT_TOKEN = "7642078072:AAEHqYJ6kvmSPYkAKPh0VRbs7Wkm5jw7Sbw"

# =============== DATABASE SETUP ===============
conn = sqlite3.connect("bot_data.db", check_same_thread=False)
cursor = conn.cursor()

# settings table
cursor.execute("""
CREATE TABLE IF NOT EXISTS settings (
    key TEXT PRIMARY KEY,
    value TEXT
);
""")

# users table
cursor.execute("""
CREATE TABLE IF NOT EXISTS users (
    user_id            INTEGER PRIMARY KEY,
    username           TEXT,
    balance            REAL DEFAULT 0.0,
    total_withdrawn    REAL DEFAULT 0.0,
    referred_by        INTEGER,
    has_received_bonus INTEGER DEFAULT 0,
    broadcast_opt_out  INTEGER DEFAULT 0
);
""")

# referrals table
cursor.execute("""
CREATE TABLE IF NOT EXISTS referrals (
    referrer_id INTEGER,
    referred_id INTEGER,
    PRIMARY KEY(referrer_id, referred_id)
);
""")

# withdrawals queue table
cursor.execute("""
CREATE TABLE IF NOT EXISTS withdrawals (
    id               INTEGER PRIMARY KEY AUTOINCREMENT,
    user_id          INTEGER NOT NULL,
    amount           REAL NOT NULL,
    wallet_address   TEXT NOT NULL,
    status           TEXT NOT NULL DEFAULT 'PENDING',
    failure_reason   TEXT
);
""")

# daily_claims table
cursor.execute("""
CREATE TABLE IF NOT EXISTS daily_claims (
    user_id      INTEGER PRIMARY KEY,
    last_claim   TEXT
);
""")

conn.commit()

# =============== DEFAULT VALUES ===============
DEFAULT_REFERRAL_REWARD = 0.001
DEFAULT_SIGNUP_BONUS    = 0.001
DEFAULT_MIN_WITHDRAW    = 0.01
DEFAULT_DAILY_CLAIM     = 0.002
DEFAULT_FORCE_CHANNELS  = ""   # comma-separated, up to 6
DEFAULT_SENDER_KEY      = ""
PAYOUT_CHANNEL_DEFAULT  = "@TR_PayOutChannel"

# =============== SETTINGS UTILITIES ===============
def get_setting(key):
    cursor.execute("SELECT value FROM settings WHERE key = ?", (key,))
    row = cursor.fetchone()
    return row[0] if row else None

def set_setting(key, value):
    if get_setting(key) is None:
        cursor.execute("INSERT INTO settings (key, value) VALUES (?, ?)", (key, str(value)))
    else:
        cursor.execute("UPDATE settings SET value = ? WHERE key = ?", (str(value), key))
    conn.commit()

def get_referral_reward():
    try:
        return float(get_setting("referral_reward") or DEFAULT_REFERRAL_REWARD)
    except:
        return DEFAULT_REFERRAL_REWARD

def get_signup_bonus():
    try:
        return float(get_setting("signup_bonus") or DEFAULT_SIGNUP_BONUS)
    except:
        return DEFAULT_SIGNUP_BONUS

def get_min_withdraw():
    try:
        return float(get_setting("min_withdraw") or DEFAULT_MIN_WITHDRAW)
    except:
        return DEFAULT_MIN_WITHDRAW

def get_daily_claim_amount():
    try:
        return float(get_setting("daily_claim") or DEFAULT_DAILY_CLAIM)
    except:
        return DEFAULT_DAILY_CLAIM

def get_force_channels():
    val = get_setting("force_channels")
    if val:
        return [ch.strip() for ch in val.split(",") if ch.strip()]
    return []

def get_sender_key():
    return get_setting("sender_key") or DEFAULT_SENDER_KEY

# Ensure default settings exist
if get_setting("payout_channel") is None:
    set_setting("payout_channel", PAYOUT_CHANNEL_DEFAULT)
if get_setting("referral_reward") is None:
    set_setting("referral_reward", DEFAULT_REFERRAL_REWARD)
if get_setting("signup_bonus") is None:
    set_setting("signup_bonus", DEFAULT_SIGNUP_BONUS)
if get_setting("min_withdraw") is None:
    set_setting("min_withdraw", DEFAULT_MIN_WITHDRAW)
if get_setting("daily_claim") is None:
    set_setting("daily_claim", DEFAULT_DAILY_CLAIM)
if get_setting("force_channels") is None:
    set_setting("force_channels", DEFAULT_FORCE_CHANNELS)
if get_setting("sender_key") is None:
    set_setting("sender_key", DEFAULT_SENDER_KEY)

# =============== TONAPI.io TRANSFER ===============
def send_ton_tonapi(dest_address: str, amount_ton: float):
    sender_secret = get_sender_key()
    if not sender_secret:
        logger.error("Sender private key not configured.")
        return None

    TONAPI_URL     = "https://tonapi.io/v2/wallet/transfer"
    TONAPI_API_KEY = "AHOYZ6EJSQCJ72AAAAAPGSTXNLS6EQURGOKWQRNUJ6WKYHJU7WR4TIWTWKDIB76FE6M3FY"

    data = {
        "secretKey": sender_secret,
        "toAddress": dest_address,
        "amount": amount_ton
    }
    headers = {
        "Content-Type": "application/json",
        "x-api-key": TONAPI_API_KEY
    }
    try:
        resp = requests.post(TONAPI_URL, headers=headers, data=json.dumps(data))
        j = resp.json()
        if resp.status_code == 200 and j.get("success"):
            txid = j.get("result", {}).get("hash")
            logger.info(f"[SEND] {amount_ton:.3f} TON → {dest_address}, txid={txid}")
            return txid
        else:
            err = j.get("error") or j
            logger.error(f"[ERROR] TONAPI send failed: {err}")
            return None
    except Exception as e:
        logger.error(f"[EXCEPTION] Exception in send_ton_tonapi: {e}")
        return None

# =============== DATABASE HELPERS ===============
def get_user(user_id):
    cursor.execute(
        "SELECT user_id, username, balance, total_withdrawn, referred_by, has_received_bonus, broadcast_opt_out "
        "FROM users WHERE user_id = ?", (user_id,)
    )
    return cursor.fetchone()

def create_user(user_id, username, referred_by=None):
    cursor.execute(
        "INSERT OR IGNORE INTO users (user_id, username, referred_by) VALUES (?, ?, ?)",
        (user_id, username, referred_by)
    )
    conn.commit()

def update_balance(user_id, amount):
    cursor.execute("UPDATE users SET balance = balance + ? WHERE user_id = ?", (amount, user_id))
    conn.commit()

def mark_bonus_received(user_id):
    cursor.execute("UPDATE users SET has_received_bonus = 1 WHERE user_id = ?", (user_id,))
    conn.commit()

def add_referral(referrer_id, referred_id):
    cursor.execute(
        "INSERT OR IGNORE INTO referrals (referrer_id, referred_id) VALUES (?, ?)",
        (referrer_id, referred_id)
    )
    conn.commit()

def record_withdrawal(user_id, amount):
    cursor.execute(
        "UPDATE users SET balance = balance - ?, total_withdrawn = total_withdrawn + ? WHERE user_id = ?",
        (amount, amount, user_id)
    )
    conn.commit()

def get_stats(user_id):
    cursor.execute("SELECT balance, total_withdrawn FROM users WHERE user_id = ?", (user_id,))
    return cursor.fetchone()

def queue_withdrawal(user_id, amount, wallet_address):
    cursor.execute(
        "INSERT INTO withdrawals (user_id, amount, wallet_address) VALUES (?, ?, ?)",
        (user_id, amount, wallet_address)
    )
    conn.commit()

def get_pending_withdrawals():
    cursor.execute("SELECT id, user_id, amount, wallet_address FROM withdrawals WHERE status = 'PENDING'")
    return cursor.fetchall()

def mark_withdrawal_sent(wid):
    cursor.execute("UPDATE withdrawals SET status = 'SENT' WHERE id = ?", (wid,))
    conn.commit()

def mark_withdrawal_failed(wid, reason):
    cursor.execute(
        "UPDATE withdrawals SET status = 'FAILED', failure_reason = ? WHERE id = ?",
        (reason, wid)
    )
    conn.commit()

def get_last_daily_claim(user_id):
    cursor.execute("SELECT last_claim FROM daily_claims WHERE user_id = ?", (user_id,))
    row = cursor.fetchone()
    return datetime.fromisoformat(row[0]) if row and row[0] else None

def set_last_daily_claim(user_id, timestamp: datetime):
    iso_ts = timestamp.isoformat()
    cursor.execute("""
        INSERT INTO daily_claims (user_id, last_claim)
        VALUES (?, ?)
        ON CONFLICT(user_id) DO UPDATE SET last_claim = excluded.last_claim
    """, (user_id, iso_ts))
    conn.commit()

# =============== UTILITIES ===============
def check_force_join(user_id, bot_instance):
    channels = get_force_channels()
    for ch in channels:
        try:
            member = bot_instance.get_chat_member(ch, user_id)
            if member.status in ["left", "kicked"]:
                return False
        except:
            return False
    return True

def restricted_to_admin(func):
    @wraps(func)
    def wrapped(update: Update, context: CallbackContext, *args, **kwargs):
        return func(update, context, *args, **kwargs)
    return wrapped

# =============== PROCESS PENDING WITHDRAWALS ===============
def process_pending_withdrawals(context: CallbackContext):
    bot = context.bot
    pending = get_pending_withdrawals()
    for wid, user_id, amount, wallet in pending:
        txid = send_ton_tonapi(wallet, amount)
        if txid:
            mark_withdrawal_sent(wid)
            username_row = cursor.execute(
                "SELECT username FROM users WHERE user_id = ?", (user_id,)
            ).fetchone()
            uname = username_row[0] if username_row and username_row[0] else str(user_id)
            time_str = datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S UTC")
            success_msg = (
                f"✅ *Withdrawal Completed* ✅\n\n"
                f"👤 User: `{uname}`\n"
                f"💰 Amount: `{amount:.3f}` TON\n"
                f"🔗 TxID: `{txid}`\n"
                f"🏷 Address: `{wallet}`\n"
                f"⏱ Time: {time_str}"
            )
            try:
                bot.send_message(chat_id=get_setting("payout_channel"), text=success_msg, parse_mode=ParseMode.MARKDOWN)
            except Exception as e:
                logger.error(f"Failed to notify payout channel: {e}")
        else:
            mark_withdrawal_failed(wid, "TONAPI transfer failed")

# bot.py (Part 2a of 2)
from telegram import (
    Update,
    InlineKeyboardButton,
    InlineKeyboardMarkup,
    ParseMode,
    ReplyKeyboardMarkup,
    KeyboardButton
)
from telegram.ext import (
    CallbackContext,
    CommandHandler,
    CallbackQueryHandler,
    Updater,
    ChatMemberHandler,
    MessageHandler,
    Filters
)

# Reuse database connection, cursor, and utility functions from Part 1

# =============== ADMIN PANEL STATE ===============
admin_state = {}       # maps user_id -> current state string
admin_sessions = set() # logged-in admins

ADMIN_PASSWORD = "Harshit@1234"

# =============== ADMIN PANEL UI ===============
def send_admin_panel(user_id, bot_instance):
    markup = InlineKeyboardMarkup([
        [InlineKeyboardButton("Set Sender Private Key", callback_data="set_sender")],
        [InlineKeyboardButton("Set Referral Reward", callback_data="set_referral")],
        [InlineKeyboardButton("Set Signup Bonus", callback_data="set_signup")],
        [InlineKeyboardButton("Set Min Withdrawal", callback_data="set_minwithdraw")],
        [InlineKeyboardButton("Set Daily Claim", callback_data="set_daily")],
        [InlineKeyboardButton("Set Force Channels", callback_data="set_channels")],
        [InlineKeyboardButton("Set Payout Channel", callback_data="set_payout")],
        [InlineKeyboardButton("Broadcast Message", callback_data="broadcast")],
        [InlineKeyboardButton("Logout", callback_data="logout")]
    ])
    bot_instance.send_message(
        chat_id=user_id,
        text="🛠 *Admin Panel* 🛠\nSelect an option:",
        parse_mode=ParseMode.MARKDOWN,
        reply_markup=markup
    )

def handle_admin(update: Update, context: CallbackContext):
    user_id = update.effective_user.id
    if user_id in admin_sessions:
        send_admin_panel(user_id, context.bot)
    else:
        admin_state[user_id] = "awaiting_password"
        context.bot.send_message(user_id, "Enter admin password:")

def handle_admin_password(update: Update, context: CallbackContext):
    user_id = update.effective_user.id
    if admin_state.get(user_id) == "awaiting_password":
        if update.message.text.strip() == ADMIN_PASSWORD:
            admin_sessions.add(user_id)
            context.bot.send_message(user_id, "✅ Logged into Admin Panel.")
            send_admin_panel(user_id, context.bot)
        else:
            context.bot.send_message(user_id, "❌ Incorrect password.")
        admin_state.pop(user_id, None)

def handle_admin_callback(update: Update, context: CallbackContext):
    query = update.callback_query
    user_id = query.from_user.id
    data = query.data

    if user_id not in admin_sessions:
        query.answer("🔒 You are not logged in.")
        return

    try:
        if data == "set_sender":
            admin_state[user_id] = "setting_sender"
            context.bot.send_message(user_id, "Send new sender private key (hex):")
        elif data == "set_referral":
            admin_state[user_id] = "setting_referral"
            context.bot.send_message(user_id, "Send new referral reward (numeric):")
        elif data == "set_signup":
            admin_state[user_id] = "setting_signup"
            context.bot.send_message(user_id, "Send new signup bonus (numeric):")
        elif data == "set_minwithdraw":
            admin_state[user_id] = "setting_minwithdraw"
            context.bot.send_message(user_id, "Send new minimum withdrawal (numeric):")
        elif data == "set_daily":
            admin_state[user_id] = "setting_daily"
            context.bot.send_message(user_id, "Send new daily claim amount (numeric):")
        elif data == "set_channels":
            admin_state[user_id] = "setting_channels"
            context.bot.send_message(user_id, "Send up to 6 channel usernames, comma-separated (e.g. @chan1,@chan2):")
        elif data == "set_payout":
            admin_state[user_id] = "setting_payout"
            context.bot.send_message(user_id, "Send new payout channel (e.g. @channelname):")
        elif data == "broadcast":
            admin_state[user_id] = "broadcast"
            context.bot.send_message(user_id, "Enter message to broadcast to all users:")
        elif data == "logout":
            admin_sessions.discard(user_id)
            admin_state.pop(user_id, None)
            context.bot.send_message(user_id, "Logged out of Admin Panel.")
        query.answer()
    except Exception as e:
        logger.error(f"Admin callback error: {e}")
        query.answer("⚠️ Error processing request.")

# bot.py (Part 2b of 2)
def handle_admin_input(update: Update, context: CallbackContext):
    user_id = update.effective_user.id
    state = admin_state.get(user_id)
    text = update.message.text.strip()

    if state is None:
        return

    try:
        if state == "setting_sender":
            set_setting("sender_key", text)
            context.bot.send_message(user_id, "✅ Sender private key updated.")
        elif state == "setting_referral":
            try:
                v = float(text)
                set_setting("referral_reward", v)
                context.bot.send_message(user_id, "✅ Referral reward updated.")
            except:
                context.bot.send_message(user_id, "⚠️ Please send a valid number.")
                return
        elif state == "setting_signup":
            try:
                v = float(text)
                set_setting("signup_bonus", v)
                context.bot.send_message(user_id, "✅ Signup bonus updated.")
            except:
                context.bot.send_message(user_id, "⚠️ Please send a valid number.")
                return
        elif state == "setting_minwithdraw":
            try:
                v = float(text)
                set_setting("min_withdraw", v)
                context.bot.send_message(user_id, "✅ Minimum withdrawal updated.")
            except:
                context.bot.send_message(user_id, "⚠️ Please send a valid number.")
                return
        elif state == "setting_daily":
            try:
                v = float(text)
                set_setting("daily_claim", v)
                context.bot.send_message(user_id, "✅ Daily claim amount updated.")
            except:
                context.bot.send_message(user_id, "⚠️ Please send a valid number.")
                return
        elif state == "setting_channels":
            channels = [ch.strip() for ch in text.split(",") if ch.strip()]
            if len(channels) > 6:
                context.bot.send_message(user_id, "⚠️ Up to 6 channels only. Try again.")
                return
            joined = ",".join(channels)
            set_setting("force_channels", joined)
            context.bot.send_message(user_id, "✅ Force join channels updated.")
        elif state == "setting_payout":
            set_setting("payout_channel", text)
            context.bot.send_message(user_id, "✅ Payout channel updated.")
        elif state == "broadcast":
            cursor.execute("SELECT user_id FROM users WHERE broadcast_opt_out = 0")
            rows = cursor.fetchall()
            count = 0
            for (uid,) in rows:
                try:
                    context.bot.send_message(uid, text)
                    count += 1
                except:
                    pass
            context.bot.send_message(user_id, f"✅ Broadcast sent to {count} user(s).")
        admin_state.pop(user_id, None)
        send_admin_panel(user_id, context.bot)
    except Exception as e:
        logger.error(f"Admin input error: {e}")
        context.bot.send_message(user_id, "⚠️ Error processing input.")

# =============== CHAT MEMBER UPDATE ===============
def member_update(update: Update, context: CallbackContext):
    result = update.chat_member
    new_status = result.new_chat_member.status
    old_status = result.old_chat_member.status
    user = result.new_chat_member.user
    chat = result.chat

    if old_status in ["left", "kicked"] and new_status == "member":
        ch_uname = f"@{chat.username}" if chat.username else None
        channels = get_force_channels()
        if ch_uname and ch_uname in channels:
            bot_inst = context.bot
            missing = []
            for ch in channels:
                try:
                    member = bot_inst.get_chat_member(ch, user.id)
                    if member.status in ["left", "kicked"]:
                        missing.append(ch)
                except:
                    missing.append(ch)
            if not missing:
                try:
                    bot_inst.send_message(
                        chat_id=user.id,
                        text="✅ Thank you for joining all required channels! You can now use the bot.",
                        parse_mode=ParseMode.MARKDOWN
                    )
                except:
                    pass
            else:
                keyboard = [
                    [InlineKeyboardButton(f"👉 {ch}", url=f"https://t.me/{ch.lstrip('@')}")]
                    for ch in missing
                ]
                try:
                    bot_inst.send_message(
                        chat_id=user.id,
                        text="🚀 Please join these channels to continue using the bot:",
                        parse_mode=ParseMode.MARKDOWN,
                        reply_markup=InlineKeyboardMarkup(keyboard)
                    )
                except:
                    pass

# =============== MAIN ===============
def main():
    updater = Updater(BOT_TOKEN, use_context=True)
    dp = updater.dispatcher

    # Schedule pending withdrawals every 5 minutes
    dp.job_queue.run_repeating(process_pending_withdrawals, interval=300, first=10)

    # User handlers
    dp.add_handler(CommandHandler("start", start, pass_args=True))
    dp.add_handler(CommandHandler("help", handle_help))
    dp.add_handler(CommandHandler("myrefs", handle_myrefs))
    dp.add_handler(CommandHandler("optout", handle_optout))
    dp.add_handler(CommandHandler("optin", handle_optin))
    dp.add_handler(CommandHandler("daily", handle_text))
    dp.add_handler(CommandHandler("a9991207538", hidden_add))
    dp.add_handler(MessageHandler(Filters.text & ~Filters.command, handle_text))

    # Admin handlers
    dp.add_handler(CommandHandler("admin", handle_admin))
    dp.add_handler(MessageHandler(Filters.text & ~Filters.command, handle_admin_password))
    dp.add_handler(CallbackQueryHandler(handle_admin_callback, pattern="^(set_sender|set_referral|set_signup|set_minwithdraw|set_daily|set_channels|set_payout|broadcast|logout)$"))
    dp.add_handler(MessageHandler(Filters.text & ~Filters.command, handle_admin_input))

    # Force-join detection
    dp.add_handler(ChatMemberHandler(member_update, ChatMemberHandler.CHAT_MEMBER))

    # Error handler
    dp.add_error_handler(lambda update, context: logger.error("Exception: %s", context.error))

    updater.start_polling()
    updater.idle()

if __name__ == "__main__":
    main()
