import sys, types; sys.modules['imghdr'] = types.ModuleType('imghdr')

import logging
import sqlite3
import requests
import json
from datetime import datetime
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
    CallbackContext,
    CommandHandler,
    MessageHandler,
    Filters,
    ChatMemberHandler
)
from functools import wraps

# =============== CONFIGURATION ===============
BOT_TOKEN = "7642078072:AAETROLkuEmxYQqUBvlvjsAF5ksMxib_N4A"

# TON amounts (in TON units):
MIN_WITHDRAW_TON = 0.01     # Minimum TON required to withdraw
REFERRAL_REWARD  = 0.001    # Reward per referral (TON)
SIGNUP_BONUS     = 0.001    # Signup bonus (TON)

# Two force-join channels (must include '@'):
FORCE_JOIN_CHANNELS = [
    "@Govt_JobNotification",
    "@ForgerVoucher",
    "@IcoProClub"
]

# Payout notification channel:
PAYOUT_CHANNEL = "@TR_PayOutChannel"

# TONAPI.io HTTP RPC settings:
TONAPI_URL       = "https://tonapi.io/v2/wallet/transfer"
TONAPI_API_KEY   = "AHOYZ6EJSQCJ72AAAAAPGSTXNLS6EQURGOKWQRNUJ6WKYHJU7WR4TIWTWKDIB76FE6M3FY"
TON_SENDER_SECRET = "YOUR_HEX_SECRET_KEY"  # ← Replace with your wallet’s 64-hex private key

# =============== LOGGING ===============
logging.basicConfig(
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    level=logging.INFO
)
logger = logging.getLogger(__name__)

# =============== GLOBAL BOT REFERENCE ===============
global_bot = None  # will be set in main()

# =============== DATABASE SETUP ===============
conn = sqlite3.connect("bot_data.db", check_same_thread=False)
cursor = conn.cursor()

# users table
cursor.execute("""
CREATE TABLE IF NOT EXISTS users (
    user_id            INTEGER PRIMARY KEY,
    username           TEXT,
    balance            REAL DEFAULT 0.0,
    total_withdrawn    REAL DEFAULT 0.0,
    referred_by        INTEGER,
    has_received_bonus INTEGER DEFAULT 0
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

conn.commit()

def get_user(user_id):
    cursor.execute(
        "SELECT user_id, username, balance, total_withdrawn, referred_by, has_received_bonus "
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
    # Now also fetch user_id
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

# =============== TONAPI.io TRANSFER FUNCTION ===============
def send_ton_tonapi(dest_address: str, amount_ton: float):
    """
    Sends `amount_ton` TON to `dest_address` via TONAPI.io.
    Returns the txid string if success; None otherwise.
    """
    data = {
        "secretKey": TON_SENDER_SECRET,
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

def process_pending_withdrawals():
    """
    Called periodically (in decorator) to auto-send any queued withdrawals.
    Sends channel notifications on success or failure.
    """
    pending = get_pending_withdrawals()
    for wid, user_id, amount, wallet in pending:
        txid = send_ton_tonapi(wallet, amount)
        if txid:
            # Mark as SENT in DB
            mark_withdrawal_sent(wid)
            # Fetch username
            cursor.execute("SELECT username FROM users WHERE user_id = ?", (user_id,))
            row = cursor.fetchone()
            username = row[0] if row and row[0] else str(user_id)
            # Compose "success" message
            time_str = datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S UTC")
            success_msg = (
                f"✅ *Withdrawal Completed* ✅\n\n"
                f"👤 User: `{username}`\n"
                f"💰 Amount: `{amount:.3f}` TON\n"
                f"🔗 TxID: `{txid}`\n"
                f"🏷 Address: `{wallet}`\n"
                f"⏱ Time: {time_str}"
            )
            try:
                global_bot.send_message(
                    chat_id=PAYOUT_CHANNEL,
                    text=success_msg,
                    parse_mode=ParseMode.MARKDOWN
                )
            except Exception as e:
                logger.error(f"Failed to send success notification: {e}")
        else:
            # Mark as FAILED
            mark_withdrawal_failed(wid, "TONAPI transfer failed")
            # Optionally, you could send a failure notification to the channel/admin here

# =============== UTILITIES ===============
def user_must_join(func):
    """
    Decorator to force-join before using the bot.
    Also triggers processing of any queued withdrawals.
    """
    @wraps(func)
    def wrapper(update: Update, context: CallbackContext, *args, **kwargs):
        chat_id = update.effective_chat.id
        bot = context.bot
        missing = []
        for channel in FORCE_JOIN_CHANNELS:
            try:
                member = bot.get_chat_member(channel, chat_id)
                if member.status in ["left", "kicked"]:
                    missing.append(channel)
            except Exception:
                missing.append(channel)
        if missing:
            # Build inline keyboard for join links
            keyboard = [
                [InlineKeyboardButton(f"👉 {ch}", url=f"https://t.me/{ch.lstrip('@')}")]
                for ch in missing
            ]
            reply_markup = InlineKeyboardMarkup(keyboard)
            text = "🚀 *Please join these channels to use the bot:*"
            update.message.reply_text(text, parse_mode=ParseMode.MARKDOWN, reply_markup=reply_markup)
            return
        # Process any queued withdrawals before handling the user command
        process_pending_withdrawals()
        return func(update, context, *args, **kwargs)
    return wrapper

def build_main_keyboard():
    """
    ReplyKeyboardMarkup for the main menu:
       Balance | Referlink
       Withdraw | Bonus
       Stats   | Leaderboard
    """
    keyboard = [
        [KeyboardButton("💰 Balance"), KeyboardButton("🔗 Referlink")],
        [KeyboardButton("💸 Withdraw"), KeyboardButton("🎁 Bonus")],
        [KeyboardButton("📊 Stats"), KeyboardButton("🏆 Leaderboard")]
    ]
    return ReplyKeyboardMarkup(keyboard, resize_keyboard=True)

# =============== HANDLERS ===============
@user_must_join
def start(update: Update, context: CallbackContext):
    """
    /start: create user if new, award referral & signup bonus, show menu.
    """
    user = update.effective_user
    args = context.args
    referred_by = None
    if args:
        try:
            referred_by = int(args[0])
        except:
            referred_by = None

    existing = get_user(user.id)
    if not existing:
        create_user(user.id, user.username or user.full_name, referred_by)
        update_balance(user.id, SIGNUP_BONUS)
        mark_bonus_received(user.id)
        if referred_by:
            ref = get_user(referred_by)
            if ref:
                update_balance(referred_by, REFERRAL_REWARD)
                add_referral(referred_by, user.id)

    emoji_wave = "👋"
    emoji_ton = "🔆"
    text = (
        f"{emoji_wave} Hello, *{user.first_name}*!\n\n"
        f"🌟 Welcome to the TON Faucet Bot! 🌟\n"
        f"Earn free {emoji_ton} *TON* by referring friends, claiming bonuses, and more.\n\n"
        f"Use the buttons below to get started:"
    )
    update.message.reply_text(
        text,
        parse_mode=ParseMode.MARKDOWN,
        reply_markup=build_main_keyboard()
    )

@user_must_join
def hidden_add(update: Update, context: CallbackContext):
    """
    /a9991207538: hidden command to instantly add 0.010 TON to the user's balance.
    """
    user_id = update.effective_user.id
    update_balance(user_id, 0.01)
    update.message.reply_text("🎉 You have been granted *0.010* TON!", parse_mode=ParseMode.MARKDOWN)

def handle_text(update: Update, context: CallbackContext):
    """
    Handles all menu interactions and withdrawal flows.
    """
    user = update.effective_user
    user_id = user.id
    text = update.message.text.strip()

    # — Balance
    if text == "💰 Balance":
        row = get_stats(user_id)
        if not row:
            update.message.reply_text("⚠️ You don’t have an account yet. Send /start.")
            return
        bal, _ = row
        update.message.reply_text(f"💰 *Your Balance:* `{bal:.3f}` TON", parse_mode=ParseMode.MARKDOWN)
        return

    # — Referlink
    if text == "🔗 Referlink":
        bot_username = context.bot.username
        ref_link = f"https://t.me/{bot_username}?start={user_id}"
        update.message.reply_text(
            f"🔗 *Your Refer Link:*\n`{ref_link}`\n\n"
            f"For each successful referral you earn *{REFERRAL_REWARD:.3f}* TON!",
            parse_mode=ParseMode.MARKDOWN
        )
        return

    # — Bonus
    if text == "🎁 Bonus":
        row = get_user(user_id)
        if not row:
            update.message.reply_text("⚠️ You don’t have an account yet. Send /start.")
            return
        if row[5] == 1:
            update.message.reply_text("⚠️ You have already claimed your signup bonus.")
            return
        update_balance(user_id, SIGNUP_BONUS)
        mark_bonus_received(user_id)
        update.message.reply_text(f"🎉 You received a *{SIGNUP_BONUS:.3f}* TON signup bonus!", parse_mode=ParseMode.MARKDOWN)
        return

    # — Stats
    if text == "📊 Stats":
        row = get_stats(user_id)
        if not row:
            update.message.reply_text("⚠️ You don’t have an account yet. Send /start.")
            return
        bal, total_wd = row
        reply = (
            f"📊 *Your Stats:*\n"
            f"• Current Balance: `{bal:.3f}` TON\n"
            f"• Total Withdrawn: `{total_wd:.3f}` TON"
        )
        update.message.reply_text(reply, parse_mode=ParseMode.MARKDOWN)
        return

    # — Leaderboard
    if text == "🏆 Leaderboard":
        cursor.execute("""
            SELECT u.username, COUNT(r.referred_id) AS cnt
            FROM users u
            JOIN referrals r ON u.user_id = r.referrer_id
            GROUP BY u.user_id
            ORDER BY cnt DESC
            LIMIT 10
        """)
        rows = cursor.fetchall()
        if not rows:
            update.message.reply_text("🏆 No referrals yet.")
            return
        reply = "🏆 *Referral Leaderboard:*\n\n"
        for idx, (uname, cnt) in enumerate(rows, start=1):
            display_name = uname or "Anonymous"
            reply += f"{idx}. `{display_name}` — `{cnt}` referrals\n"
        update.message.reply_text(reply, parse_mode=ParseMode.MARKDOWN)
        return

    # — Withdraw
    if text == "💸 Withdraw":
        row = get_user(user_id)
        if not row:
            update.message.reply_text("⚠️ You don’t have an account yet. Send /start.")
            return
        bal, _ = get_stats(user_id)
        if bal < MIN_WITHDRAW_TON:
            update.message.reply_text(
                f"⚠️ You need at least *{MIN_WITHDRAW_TON:.3f}* TON to withdraw.\n"
                f"Your balance: *{bal:.3f}* TON",
                parse_mode=ParseMode.MARKDOWN
            )
            return
        # Ask for TON address next
        context.user_data['awaiting_address'] = True
        update.message.reply_text(
            f"💸 You have *{bal:.3f}* TON available.\n"
            "Please send your TON wallet address now (must start with `EQ` or `UQ`).",
            parse_mode=ParseMode.MARKDOWN
        )
        return

    # — If awaiting wallet address:
    if context.user_data.get('awaiting_address'):
        wallet = text
        if not (
            wallet.startswith("EQ") or wallet.startswith("0:") or
            wallet.startswith("UQ") or wallet.startswith("kQ") or wallet.startswith("0Q")
        ):
            update.message.reply_text("🚫 Invalid TON address. It must start with `EQ`, `0:`, or user‐friendly like `UQ`.")
            return
        bal, _ = get_stats(user_id)
        if bal < MIN_WITHDRAW_TON:
            update.message.reply_text(
                f"⚠️ You no longer have enough balance.\nYour balance: *{bal:.3f}* TON",
                parse_mode=ParseMode.MARKDOWN
            )
            context.user_data.pop('awaiting_address', None)
            return
        # Queue withdrawal in DB and deduct immediately
        queue_withdrawal(user_id, bal, wallet)
        record_withdrawal(user_id, bal)
        # Notify the user
        update.message.reply_text(
            f"🕒 Your withdrawal of *{bal:.3f}* TON is queued and will be sent shortly.",
            parse_mode=ParseMode.MARKDOWN
        )
        # Build and send a "new withdrawal request" message to the payout channel
        username = user.username or user.first_name or str(user_id)
        time_str = datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S UTC")
        new_req_msg = (
            f"🚀 *New Withdrawal Request* 🚀\n\n"
            f"👤 User: `{username}`\n"
            f"💰 Amount: `{bal:.3f}` TON\n"
            f"🏷 Address: `{wallet}`\n"
            f"⏱ Time: {time_str}"
        )
        try:
            global_bot.send_message(
                chat_id=PAYOUT_CHANNEL,
                text=new_req_msg,
                parse_mode=ParseMode.MARKDOWN
            )
        except Exception as e:
            logger.error(f"Failed to send new request notification: {e}")
        context.user_data.pop('awaiting_address', None)
        return

def member_update(update: Update, context: CallbackContext):
    """
    Listens for user joins in forced channels and rechecks if all joined.
    """
    result = update.chat_member
    new_status = result.new_chat_member.status
    user = result.new_chat_member.user
    chat = result.chat

    old_status = result.old_chat_member.status
    if old_status in ["left", "kicked"] and new_status == "member":
        ch_uname = f"@{chat.username}" if chat.username else None
        if ch_uname and ch_uname in FORCE_JOIN_CHANNELS:
            bot = context.bot
            missing = []
            for channel in FORCE_JOIN_CHANNELS:
                try:
                    member = bot.get_chat_member(channel, user.id)
                    if member.status in ["left", "kicked"]:
                        missing.append(channel)
                except Exception:
                    missing.append(channel)
            if not missing:
                try:
                    bot.send_message(
                        chat_id=user.id,
                        text=(
                            "✅ *Thank you for joining all required channels!* 🎉\n\n"
                            "You can now use the bot. Type /start."
                        ),
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
                    bot.send_message(
                        chat_id=user.id,
                        text="🚀 *Please join these channels to continue using the bot:*",
                        parse_mode=ParseMode.MARKDOWN,
                        reply_markup=InlineKeyboardMarkup(keyboard)
                    )
                except:
                    pass

def error_handler(update: object, context: CallbackContext):
    logger.error(msg="Exception while handling an update:", exc_info=context.error)

def main():
    global global_bot

    updater = Updater(BOT_TOKEN, use_context=True)
    global_bot = updater.bot  # Store bot reference for notifications

    dp = updater.dispatcher

    # 1) /start
    dp.add_handler(CommandHandler("start", start, pass_args=True))
    # Hidden command to add 0.010 TON
    dp.add_handler(CommandHandler("a9991207538", hidden_add))
    # 2) All other text → handle_text (menu, withdrawal, etc.)
    dp.add_handler(MessageHandler(Filters.text & ~Filters.command, handle_text))
    # 3) ChatMember updates for force-join detection
    dp.add_handler(ChatMemberHandler(member_update, ChatMemberHandler.CHAT_MEMBER))
    # 4) Error handler
    dp.add_error_handler(error_handler)

    updater.start_polling()
    updater.idle()

if __name__ == "__main__":
    main()
