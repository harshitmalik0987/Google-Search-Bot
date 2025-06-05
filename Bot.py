import sys
import types
# The following line is a workaround for an issue some environments might have.
# If you don't face 'imghdr' import errors, you might not strictly need it.
sys.modules['imghdr'] = types.ModuleType('imghdr')

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
    KeyboardButton,
    CallbackQuery
)
from telegram.ext import (
    Updater,
    CallbackContext,
    CommandHandler,
    MessageHandler,
    Filters,
    ChatMemberHandler,
    ConversationHandler,
    CallbackQueryHandler
)
from functools import wraps

# =============== CONFIGURATION ===============
BOT_TOKEN = "7642078072:AAEHqYJ6kvmSPYkAKPh0VRbs7Wkm5jw7Sbw" # From your provided script

# TON amounts (in TON units):
MIN_WITHDRAW_TON = 0.01     # Minimum TON required to withdraw
REFERRAL_REWARD  = 0.001    # Reward per referral (TON)
SIGNUP_BONUS     = 0.001    # Signup bonus (TON)

# Two force-join channels (must include '@'):
FORCE_JOIN_CHANNELS = [ # From your provided script
    "@Govt_JobNotification",
    "@ForgerVoucher"
]

# Payout notification channel:
PAYOUT_CHANNEL = "@TR_PayOutChannel" # From your provided script

# TONAPI.io HTTP RPC settings:
TONAPI_URL       = "https://tonapi.io/v2/wallet/transfer"
# IMPORTANT: Replace with your actual TONAPI.io Key if the one below is a placeholder for you
TONAPI_API_KEY   = "AHOYZ6EJSQCJ72AAAAAPGSTXNLS6EQURGOKWQRNUJ6WKYHJU7WR4TIWTWKDIB76FE6M3FY" 
TON_SENDER_SECRET = "YOUR_HEX_SECRET_KEY"  # ← CRITICAL: Replace with your wallet’s 64-hex private key

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
    has_received_bonus INTEGER DEFAULT 0,
    wallet_address     TEXT DEFAULT NULL
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
    status           TEXT NOT NULL DEFAULT 'PENDING', -- PENDING, SENT, FAILED
    txid             TEXT,
    failure_reason   TEXT,
    queued_at        TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    processed_at     TIMESTAMP
);
""")

conn.commit()

# --- User data access functions ---
def get_user(user_id):
    cursor.execute(
        "SELECT user_id, username, balance, total_withdrawn, referred_by, has_received_bonus, wallet_address "
        "FROM users WHERE user_id = ?", (user_id,)
    )
    return cursor.fetchone()

def create_user(user_id, username_str, referred_by=None):
    cursor.execute(
        "INSERT OR IGNORE INTO users (user_id, username, referred_by) VALUES (?, ?, ?)",
        (user_id, username_str, referred_by)
    )
    conn.commit()

def update_balance(user_id, amount_change): # Can be positive or negative
    cursor.execute("UPDATE users SET balance = balance + ? WHERE user_id = ?", (amount_change, user_id))
    conn.commit()

def mark_bonus_received(user_id):
    cursor.execute("UPDATE users SET has_received_bonus = 1 WHERE user_id = ?", (user_id,))
    conn.commit()

def set_user_wallet(user_id, wallet_address):
    cursor.execute("UPDATE users SET wallet_address = ? WHERE user_id = ?", (wallet_address, user_id))
    conn.commit()

def get_user_wallet(user_id):
    cursor.execute("SELECT wallet_address FROM users WHERE user_id = ?", (user_id,))
    result = cursor.fetchone()
    return result[0] if result and result[0] else None

def add_referral(referrer_id, referred_id):
    cursor.execute(
        "INSERT OR IGNORE INTO referrals (referrer_id, referred_id) VALUES (?, ?)",
        (referrer_id, referred_id)
    )
    conn.commit()

def record_withdrawal_details(user_id, amount):
    cursor.execute(
        "UPDATE users SET balance = balance - ?, total_withdrawn = total_withdrawn + ? WHERE user_id = ?",
        (amount, amount, user_id)
    )
    conn.commit()

def get_stats(user_id):
    cursor.execute("SELECT balance, total_withdrawn FROM users WHERE user_id = ?", (user_id,))
    return cursor.fetchone()

# --- Withdrawal queue functions ---
def queue_withdrawal(user_id, amount, wallet_address):
    cursor.execute(
        "INSERT INTO withdrawals (user_id, amount, wallet_address) VALUES (?, ?, ?)",
        (user_id, amount, wallet_address)
    )
    conn.commit()
    logger.info(f"Withdrawal of {amount:.6f} TON for user {user_id} to {wallet_address} queued.")

def get_pending_withdrawals():
    cursor.execute("SELECT id, user_id, amount, wallet_address FROM withdrawals WHERE status = 'PENDING'")
    return cursor.fetchall()

def mark_withdrawal_sent(wid, txid):
    cursor.execute(
        "UPDATE withdrawals SET status = 'SENT', txid = ?, processed_at = CURRENT_TIMESTAMP WHERE id = ?",
        (txid, wid)
    )
    conn.commit()

def mark_withdrawal_failed(wid, reason):
    cursor.execute(
        "UPDATE withdrawals SET status = 'FAILED', failure_reason = ?, processed_at = CURRENT_TIMESTAMP WHERE id = ?",
        (reason, wid)
    )
    conn.commit()

# ===== END OF PART 1 =====
# ===== START OF PART 2 =====
# (Ensure Part 1 is above this in your final script)

# =============== TONAPI.io TRANSFER FUNCTION ===============
def send_ton_tonapi(dest_address: str, amount_ton: float):
    if TON_SENDER_SECRET == "YOUR_HEX_SECRET_KEY" or not TON_SENDER_SECRET:
        logger.error("CRITICAL: TON_SENDER_SECRET is not set or is invalid. Cannot send TON.")
        return None
    if not TONAPI_API_KEY:
        logger.error("CRITICAL: TONAPI_API_KEY is not set. Cannot initiate TON transfer.")
        return None


    # As per original code, amount is float TON. TONAPI.io might expect nanotons.
    # This implementation follows the original assumption that API takes float TON.
    data_for_api = {
        "secretKey": TON_SENDER_SECRET,
        "toAddress": dest_address,
        "amount": amount_ton 
    }
    headers_for_api = {
        "Content-Type": "application/json",
        "x-api-key": TONAPI_API_KEY # Using x-api-key as per original code structure
    }

    try:
        logger.info(f"Attempting to send {amount_ton:.6f} TON to {dest_address} via TONAPI.")
        resp = requests.post(TONAPI_URL, headers=headers_for_api, data=json.dumps(data_for_api))
        
        # Log raw response for debugging if needed, especially for unexpected structures
        # logger.debug(f"TONAPI Response Status: {resp.status_code}, Body: {resp.text}")

        resp.raise_for_status() # Raises HTTPError for 4xx/5xx responses
        j = resp.json()

        # TONAPI.io /v2/wallet/transfer success and txid extraction:
        # The original code checked for j.get("success") and j.get("result", {}).get("hash")
        # Let's be flexible based on common API patterns.
        txid = None
        if resp.status_code == 200: # Check for HTTP 200 OK
            if j.get("success") is True: # Explicit check for boolean true
                result_data = j.get("result")
                if isinstance(result_data, dict):
                    txid = result_data.get("hash")
                elif isinstance(result_data, str): # Sometimes result might just be the hash string
                    txid = result_data
            elif j.get("ok") is True: # Alternative success flag
                result_data = j.get("result") # Common pattern
                if isinstance(result_data, dict):
                    txid = result_data.get("hash")
                elif isinstance(result_data, str):
                    txid = result_data
                # If no specific 'hash' field, but a transaction field exists
                elif 'transaction' in j and isinstance(j['transaction'], dict):
                     txid = j['transaction'].get('hash') # Example path
            # Add more specific checks if exact TONAPI response structure is known
            # Fallback if no clear success=true but 200 OK and some hash-like field exists at top level
            if not txid and (j.get("hash") or j.get("tx_hash")):
                txid = j.get("hash") or j.get("tx_hash")


        if txid:
            logger.info(f"[SEND_SUCCESS] {amount_ton:.6f} TON → {dest_address}, txid={txid}")
            return txid
        else:
            err_detail = j.get("error", "TXID not found or unspecified error in successful response.")
            logger.error(f"[SEND_FAIL] TONAPI call seemed successful (status {resp.status_code}) but TXID missing or error indicated. Detail: {err_detail}. Full Response: {j}")
            return None
            
    except requests.exceptions.HTTPError as http_err:
        response_text = http_err.response.text if http_err.response else 'No response body'
        logger.error(f"[HTTP_EXCEPTION] HTTP error in send_ton_tonapi to {dest_address} for {amount_ton:.6f} TON: {http_err} - Response: {response_text}")
        return None
    except requests.exceptions.RequestException as req_err: # More general network issues
        logger.error(f"[REQUEST_EXCEPTION] Request error in send_ton_tonapi to {dest_address} for {amount_ton:.6f} TON: {req_err}")
        return None
    except json.JSONDecodeError as json_err:
        logger.error(f"[JSON_EXCEPTION] Failed to decode JSON response from TONAPI for {dest_address}, {amount_ton:.6f} TON. Error: {json_err}. Response text: {resp.text if 'resp' in locals() else 'N/A'}")
        return None
    except Exception as e:
        logger.error(f"[UNEXPECTED_EXCEPTION] Unexpected error in send_ton_tonapi for {dest_address}, {amount_ton:.6f} TON: {e}")
        return None

def process_pending_withdrawals():
    if TON_SENDER_SECRET == "YOUR_HEX_SECRET_KEY" or not TON_SENDER_SECRET:
        # This state is logged at bot startup; no need for repeated logs here unless for debugging
        # logger.debug("Withdrawal processing skipped: TON_SENDER_SECRET is not configured.")
        return

    pending = get_pending_withdrawals()
    if not pending:
        return
    logger.info(f"Processing {len(pending)} pending withdrawal(s)...")

    for wid, user_id, amount, wallet in pending:
        logger.info(f"Processing withdrawal ID {wid}: User {user_id}, Amount {float(amount):.6f}, Wallet {wallet}")
        txid = send_ton_tonapi(wallet, float(amount)) # Ensure amount is float for the API call

        if txid:
            mark_withdrawal_sent(wid, txid)
            user_data_tuple = get_user(user_id) # Returns tuple: (user_id, username, balance, ...)
            username_for_msg = "Unknown User"
            if user_data_tuple and user_data_tuple[1]: # username is at index 1
                username_for_msg = f"@{user_data_tuple[1]}"
            else: # Fallback if username is None
                username_for_msg = f"User ID: {user_id}"

            time_str = datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S UTC")
            success_msg = (
                f"✅ *Withdrawal Completed* ✅\n\n"
                f"👤 User: `{username_for_msg}`\n"
                f"💰 Amount: `{float(amount):.6f}` TON\n"
                f"🔗 TxID: `{txid}`\n"
                f"🏷 Address: `{wallet}`\n"
                f"⏱ Time: {time_str}"
            )
            try:
                if global_bot and PAYOUT_CHANNEL and PAYOUT_CHANNEL not in ["@YourPayoutChannel", "", None]:
                    global_bot.send_message(
                        chat_id=PAYOUT_CHANNEL,
                        text=success_msg,
                        parse_mode=ParseMode.MARKDOWN
                    )
            except Exception as e:
                logger.error(f"Failed to send success notification for WID {wid} to payout channel {PAYOUT_CHANNEL}: {e}")
        else:
            failure_reason = "TONAPI transfer failed or did not return a transaction ID."
            mark_withdrawal_failed(wid, failure_reason)
            logger.error(f"Withdrawal ID {wid} for user {user_id} to wallet {wallet} failed. Reason: {failure_reason}")
            try:
                if global_bot: # Notify user of failure
                     global_bot.send_message(
                         user_id,
                         f"⚠️ Your withdrawal of {float(amount):.6f} TON to address `{wallet}` could not be processed at this time. The site admin has been notified. Please try again later or contact support if the issue persists." ,
                         parse_mode=ParseMode.MARKDOWN
                     )
            except Exception as e:
                logger.error(f"Failed to send withdrawal failure PM to user {user_id}: {e}")

# =============== UTILITIES ===============
def user_must_join(func):
    @wraps(func)
    def wrapper(update: Update, context: CallbackContext, *args, **kwargs):
        if not update or not (hasattr(update, 'effective_user') and update.effective_user):
            logger.warning("Update object missing effective_user in user_must_join decorator.")
            return ConversationHandler.END if isinstance(context, CallbackContext) else None

        user_id = update.effective_user.id
        bot = context.bot
        missing_channels = []
        
        if FORCE_JOIN_CHANNELS: # Only check if channels are configured
            for channel_name in FORCE_JOIN_CHANNELS:
                try:
                    member = bot.get_chat_member(channel_name, user_id)
                    if member.status in ["left", "kicked", "restricted"]: # Consider restricted as not adequately joined
                        missing_channels.append(channel_name)
                except Exception as e: # Bot not admin, channel deleted, user blocked bot, etc.
                    logger.warning(f"Could not check membership for user {user_id} in channel {channel_name}: {e}. Assuming not joined for safety.")
                    missing_channels.append(channel_name) # Treat error as not joined

        if missing_channels:
            keyboard_layout = [[InlineKeyboardButton(f"👉 Join {ch_name.lstrip('@')}", url=f"https://t.me/{ch_name.lstrip('@')}")] for ch_name in missing_channels]
            keyboard_layout.append([InlineKeyboardButton("✅ I've Joined All - Click to Continue", callback_data="check_join_status_main")])
            reply_markup = InlineKeyboardMarkup(keyboard_layout)
            join_message_text = "🚀 *Please join these channels to use all bot features:*"
            
            target_chat_id = update.effective_chat.id if hasattr(update, 'effective_chat') and update.effective_chat else user_id

            if update.callback_query: # If called from a callback (like the refresh button)
                try:
                    # Try editing the existing message
                    update.callback_query.edit_message_text(join_message_text, parse_mode=ParseMode.MARKDOWN, reply_markup=reply_markup)
                except Exception as e: # If edit fails (e.g., message too old, unchanged)
                    logger.info(f"Could not edit join prompt message for user {user_id} (likely unchanged or old): {e}")
                    # Send a new message as a fallback if edit fails from callback
                    context.bot.send_message(target_chat_id, join_message_text, parse_mode=ParseMode.MARKDOWN, reply_markup=reply_markup)
            elif hasattr(update, 'message') and update.message: # If called from a regular message
                update.message.reply_text(join_message_text, parse_mode=ParseMode.MARKDOWN, reply_markup=reply_markup)
            else: # Fallback if no obvious way to reply (should be rare)
                context.bot.send_message(target_chat_id, join_message_text, parse_mode=ParseMode.MARKDOWN, reply_markup=reply_markup)
            return ConversationHandler.END # End any ongoing conversation
        
        # If all checks pass, process pending withdrawals then proceed with the original function
        process_pending_withdrawals()
        return func(update, context, *args, **kwargs)
    return wrapper

def build_main_keyboard():
    keyboard = [
        [KeyboardButton("💰 Balance"), KeyboardButton("🔗 Referlink")],
        [KeyboardButton("💸 Withdraw"), KeyboardButton("🎁 Bonus")],
        [KeyboardButton("📊 Stats"), KeyboardButton("🏆 Leaderboard")],
        [KeyboardButton("⚙️ Set Wallet")]
    ]
    return ReplyKeyboardMarkup(keyboard, resize_keyboard=True, one_time_keyboard=False)

# =============== Conversation Handler States ===============
(ASK_WITHDRAW_AMOUNT, ASK_WALLET_ADDRESS, CONFIRM_WITHDRAWAL, 
 ASK_SET_WALLET, CONFIRM_SAVED_WALLET_WITHDRAWAL) = range(5)

# =============== BASIC COMMAND HANDLERS ===============
@user_must_join
def start_command(update: Update, context: CallbackContext):
    user = update.effective_user
    chat_id_to_use = update.effective_chat.id if update.effective_chat else user.id

    args = context.args if hasattr(context, 'args') else []
    referred_by = None

    if args:
        try:
            potential_referrer_id = int(args[0])
            if potential_referrer_id != user.id: # Prevent self-referral via link
                referred_by = potential_referrer_id
            else:
                logger.info(f"User {user.id} attempted self-referral via start link.")
        except ValueError:
            logger.warning(f"Invalid referral ID in /start args: '{args[0]}' for user {user.id}")
            # Avoid sending message if context.bot is not available (e.g. in tests without full setup)
            if hasattr(context, 'bot'):
                 context.bot.send_message(chat_id_to_use, "Invalid referral link format.", reply_markup=build_main_keyboard())
            return ConversationHandler.END

    db_user_tuple = get_user(user.id)
    if not db_user_tuple:
        username_str = user.username or user.full_name or str(user.id) # Ensure a string for username
        create_user(user.id, username_str, referred_by)
        logger.info(f"New user {user.id} ('{username_str}') created. Referred by: {referred_by if referred_by else 'None'}.")

        current_user_data_for_bonus = get_user(user.id) # Re-fetch after creation
        # has_received_bonus is index 5
        if SIGNUP_BONUS > 0 and current_user_data_for_bonus and not current_user_data_for_bonus[5]:
            update_balance(user.id, SIGNUP_BONUS)
            mark_bonus_received(user.id)
            if hasattr(context, 'bot'):
                context.bot.send_message(chat_id_to_use, f"🎉 You've received a *{SIGNUP_BONUS:.6f}* TON signup bonus!", parse_mode=ParseMode.MARKDOWN)

        if referred_by:
            referrer_user_tuple = get_user(referred_by)
            if referrer_user_tuple:
                update_balance(referred_by, REFERRAL_REWARD)
                add_referral(referred_by, user.id) # Record the referral
                logger.info(f"Referrer {referred_by} awarded {REFERRAL_REWARD:.6f} TON for referring user {user.id}.")
                try:
                    if hasattr(context, 'bot'):
                        context.bot.send_message(referred_by, f"🎉 You've received *{REFERRAL_REWARD:.6f}* TON! User {user.first_name or 'Anonymous'} joined via your link.", parse_mode=ParseMode.MARKDOWN)
                except Exception as e:
                    logger.error(f"Failed to send referral reward notification to referrer {referred_by}: {e}")
            else:
                 logger.warning(f"Referrer ID {referred_by} (from start link for user {user.id}) not found in DB.")
    
    emoji_wave = "👋"
    emoji_ton = "🔆" # Example emoji for TON
    welcome_text = (
        f"{emoji_wave} Hello, *{user.first_name or 'Valued User'}*!\n\n"
        f"🌟 Welcome to the TON Faucet Bot! 🌟\n"
        f"You can earn free {emoji_ton} *TON* by referring friends, claiming bonuses, and participating in activities.\n\n"
        f"Use the buttons below to navigate and start earning:"
    )
    if hasattr(context, 'bot'):
        context.bot.send_message(chat_id_to_use, welcome_text, parse_mode=ParseMode.MARKDOWN, reply_markup=build_main_keyboard())
    return ConversationHandler.END # End any previous conversation state

@user_must_join
def hidden_add_command(update: Update, context: CallbackContext):
    user_id = update.effective_user.id
    amount_to_add = 0.010 # Configurable hidden amount
    update_balance(user_id, amount_to_add)
    update.message.reply_text(f"🎉 A little surprise! You've been granted *{amount_to_add:.6f}* TON!", parse_mode=ParseMode.MARKDOWN)

@user_must_join
def balance_handler(update: Update, context: CallbackContext):
    user_id = update.effective_user.id
    db_user_tuple = get_user(user_id)
    if not db_user_tuple: # Should be handled by user_must_join if start was never called, but good check
        update.message.reply_text("⚠️ Your account isn't fully set up. Please send /start first.", reply_markup=build_main_keyboard())
        return
    balance = db_user_tuple[2] # balance is at index 2
    update.message.reply_text(f"💰 *Your Current Balance:* `{balance:.6f}` TON", parse_mode=ParseMode.MARKDOWN)

@user_must_join
def referlink_handler(update: Update, context: CallbackContext):
    user_id = update.effective_user.id
    bot_username = context.bot.username
    ref_link = f"https://t.me/{bot_username}?start={user_id}"
    update.message.reply_text(
        f"🔗 *Your Personal Referral Link:*\n`{ref_link}`\n\n"
        f"Share this link! For each friend who joins and starts the bot using your link, you'll receive *{REFERRAL_REWARD:.6f}* TON.",
        parse_mode=ParseMode.MARKDOWN
    )

@user_must_join
def bonus_handler(update: Update, context: CallbackContext):
    user_id = update.effective_user.id
    db_user_tuple = get_user(user_id)
    if not db_user_tuple:
        update.message.reply_text("⚠️ Account not found. Please use /start.", reply_markup=build_main_keyboard())
        return
    # has_received_bonus is at index 5
    if db_user_tuple[5] == 1:
        update.message.reply_text("⚠️ You have already claimed your one-time signup bonus.")
        return

    update_balance(user_id, SIGNUP_BONUS)
    mark_bonus_received(user_id) # Mark it as received
    update.message.reply_text(f"🎉 Congratulations! You've received a *{SIGNUP_BONUS:.6f}* TON signup bonus!", parse_mode=ParseMode.MARKDOWN)

@user_must_join
def stats_handler(update: Update, context: CallbackContext):
    user_id = update.effective_user.id
    stats_data_tuple = get_stats(user_id) # (balance, total_withdrawn)
    if not stats_data_tuple:
        update.message.reply_text("⚠️ No stats found. Have you used /start yet?", reply_markup=build_main_keyboard())
        return
    
    balance, total_withdrawn = stats_data_tuple
    cursor.execute("SELECT COUNT(referred_id) FROM referrals WHERE referrer_id = ?", (user_id,))
    referral_count_result = cursor.fetchone()
    referral_count = referral_count_result[0] if referral_count_result else 0

    stats_reply = (
        f"📊 *Your Personal Statistics:*\n\n"
        f"▫️ Current Balance: `{balance:.6f}` TON\n"
        f"▫️ Total Withdrawn: `{total_withdrawn:.6f}` TON\n"
        f"▫️ Friends Referred: `{referral_count}`"
    )
    update.message.reply_text(stats_reply, parse_mode=ParseMode.MARKDOWN)

@user_must_join
def leaderboard_handler(update: Update, context: CallbackContext):
    cursor.execute("""
        SELECT u.username, u.user_id, COUNT(r.referred_id) AS num_referrals
        FROM users u
        LEFT JOIN referrals r ON u.user_id = r.referrer_id
        GROUP BY u.user_id, u.username
        HAVING COUNT(r.referred_id) > 0
        ORDER BY num_referrals DESC, u.user_id ASC 
        LIMIT 10 
    """) # Added user_id for potential future use, order by referrals then ID for tie-breaking
    top_referrers = cursor.fetchall()

    if not top_referrers:
        update.message.reply_text("🏆 The referral leaderboard is currently empty. Be the first to invite friends!")
        return

    leaderboard_text = "🏆 *Top Referrers Leaderboard:*\n\n"
    for index, (username, _, num_referrals) in enumerate(top_referrers, start=1):
        display_name = username if username else f"User #{index}" # Fallback for users without usernames
        leaderboard_text += f"{index}. `{display_name}` — `{num_referrals}` referrals\n"
    
    update.message.reply_text(leaderboard_text, parse_mode=ParseMode.MARKDOWN)

# ===== END OF PART 2 ==
# ===== START OF PART 3a (Originally Start of Part 3) =====
# (Ensure Part 1 and Part 2 are above this in your final script)

# =============== CONVERSATION HANDLERS (Set Wallet, Withdraw) ===============

# --- Set Wallet Conversation ---
@user_must_join
def set_wallet_start(update: Update, context: CallbackContext):
    update.message.reply_text(
        "✏️ Please send your TON wallet address that you'd like to save for withdrawals.\n"
        "It should typically start with `EQ` or `UQ` (user-friendly format). Type /cancel to abort this process.",
        parse_mode=ParseMode.MARKDOWN
    )
    return ASK_SET_WALLET

def set_wallet_address(update: Update, context: CallbackContext):
    user_id = update.effective_user.id
    wallet_address = update.message.text.strip()

    # Basic validation for TON addresses (can be made more robust with regex)
    is_valid_address = (
        (wallet_address.startswith("EQ") or wallet_address.startswith("UQ") or \
         wallet_address.startswith("kQ") or wallet_address.startswith("0Q") or \
         wallet_address.startswith("Ef") or wallet_address.startswith("Uf") or \
         wallet_address.startswith("0:")) and \
        (40 <= len(wallet_address) <= 70) # Rough length check, actual TON addresses are 48 chars base64 + check + type
    )

    if not is_valid_address:
        update.message.reply_text(
            "🚫 That doesn't look like a valid TON address. Please check the format (e.g., starts with `EQ` or `UQ`) and try again, or type /cancel.",
            reply_markup=build_main_keyboard() # Provide main menu for easy exit
        )
        return ASK_SET_WALLET # Stay in the same state to allow another attempt

    set_user_wallet(user_id, wallet_address)
    update.message.reply_text(
        f"✅ Your TON wallet address has been successfully set to:\n`{wallet_address}`\nThis address will be suggested for future withdrawals.",
        parse_mode=ParseMode.MARKDOWN,
        reply_markup=build_main_keyboard()
    )
    return ConversationHandler.END

# --- Withdraw Conversation ---
@user_must_join
def withdraw_start(update: Update, context: CallbackContext):
    # Check if withdrawals are enabled (TON_SENDER_SECRET is set)
    if TON_SENDER_SECRET == "YOUR_HEX_SECRET_KEY" or not TON_SENDER_SECRET:
        update.message.reply_text(
            "⚠️ Withdrawals are temporarily unavailable due to system maintenance. Please check back later. We apologize for any inconvenience.",
            reply_markup=build_main_keyboard()
        )
        logger.warning(f"Withdrawal attempt by user {update.effective_user.id} blocked: TON_SENDER_SECRET is not configured.")
        return ConversationHandler.END # Exit conversation

    user_id = update.effective_user.id
    db_user_tuple = get_user(user_id)
    if not db_user_tuple: # Should be caught by decorator, but defensive check
        update.message.reply_text("⚠️ Account not found. Please /start the bot first.", reply_markup=build_main_keyboard())
        return ConversationHandler.END
    
    balance = db_user_tuple[2] # balance is index 2

    if balance < MIN_WITHDRAW_TON:
        update.message.reply_text(
            f"⚠️ Insufficient balance. You need at least *{MIN_WITHDRAW_TON:.6f}* TON to make a withdrawal.\n"
            f"Your current balance: *{balance:.6f}* TON.",
            parse_mode=ParseMode.MARKDOWN, reply_markup=build_main_keyboard()
        )
        return ConversationHandler.END

    update.message.reply_text(
        f"💸 Your current balance is *{balance:.6f}* TON.\n"
        f"Please enter the amount of TON you wish to withdraw (minimum: {MIN_WITHDRAW_TON:.6f} TON).\nType /cancel to abort.",
        parse_mode=ParseMode.MARKDOWN
    )
    return ASK_WITHDRAW_AMOUNT

def withdraw_ask_amount(update: Update, context: CallbackContext):
    user_id = update.effective_user.id
    try:
        amount_to_withdraw = float(update.message.text.strip())
        if amount_to_withdraw <= 0: raise ValueError("Amount must be positive")
    except ValueError:
        update.message.reply_text("🚫 Invalid amount. Please enter a positive numeric value (e.g., `0.5` or `10`). Or type /cancel.")
        return ASK_WITHDRAW_AMOUNT # Stay in this state

    db_user_tuple = get_user(user_id) # Re-fetch for current balance
    balance = db_user_tuple[2]

    if amount_to_withdraw < MIN_WITHDRAW_TON:
        update.message.reply_text(
            f"⚠️ Minimum withdrawal amount is *{MIN_WITHDRAW_TON:.6f}* TON. Please enter a higher amount or /cancel.",
            parse_mode=ParseMode.MARKDOWN
        )
        return ASK_WITHDRAW_AMOUNT
    if amount_to_withdraw > balance:
        update.message.reply_text(
            f"⚠️ You cannot withdraw more than your current balance of *{balance:.6f}* TON. Please enter a valid amount or /cancel.",
            parse_mode=ParseMode.MARKDOWN
        )
        return ASK_WITHDRAW_AMOUNT

    context.user_data['withdraw_amount'] = amount_to_withdraw
    saved_wallet = db_user_tuple[6] # wallet_address is index 6

    if saved_wallet:
        context.user_data['withdrawal_wallet'] = saved_wallet # Store for confirmation step
        update.message.reply_text(
            f"You are requesting to withdraw *{amount_to_withdraw:.6f}* TON.\n"
            f"Your saved wallet address is:\n`{saved_wallet}`\n\n"
            f"Type *yes* to confirm withdrawal to this address.\n"
            f"Type *new* to use a different address for this withdrawal.\n"
            f"Or type /cancel to abort.",
            parse_mode=ParseMode.MARKDOWN
        )
        return CONFIRM_SAVED_WALLET_WITHDRAWAL
    else: # No saved wallet
        update.message.reply_text(
            f"You are requesting to withdraw *{amount_to_withdraw:.6f}* TON.\n"
            f"Please send your TON wallet address for this withdrawal (e.g., starts with `EQ` or `UQ`).\nOr type /cancel.",
            parse_mode=ParseMode.MARKDOWN
        )
        return ASK_WALLET_ADDRESS

def withdraw_confirm_saved_wallet(update: Update, context: CallbackContext):
    user_id = update.effective_user.id
    choice = update.message.text.strip().lower()
    amount = context.user_data.get('withdraw_amount')

    if not amount: # Safety check
        update.message.reply_text("❌ Critical error: Withdrawal amount not found in session. Please start over with '💸 Withdraw'.", reply_markup=build_main_keyboard())
        return ConversationHandler.END

    if choice == 'yes':
        wallet_to_use = context.user_data.get('withdrawal_wallet') # Should be the saved one
        if not wallet_to_use: # Another safety check
             update.message.reply_text("❌ Critical error: Saved wallet address missing. Please try setting it again or use a new address. Start over with '💸 Withdraw'.", reply_markup=build_main_keyboard())
             return ConversationHandler.END

        # Final balance check before queueing
        current_balance = get_user(user_id)[2]
        if amount > current_balance:
            update.message.reply_text(
                f"⚠️ Your balance seems to have changed. You only have *{current_balance:.6f}* TON. Please start the withdrawal process again.",
                parse_mode=ParseMode.MARKDOWN, reply_markup=build_main_keyboard()
            )
            context.user_data.clear()
            return ConversationHandler.END
        
        queue_withdrawal(user_id, amount, wallet_to_use)
        record_withdrawal_details(user_id, amount) # Deduct balance from user's account in DB
        update.message.reply_text(
            f"✅ Your withdrawal request of *{amount:.6f}* TON to `{wallet_to_use}` has been queued and will be processed shortly!",
            parse_mode=ParseMode.MARKDOWN, reply_markup=build_main_keyboard()
        )
        logger.info(f"User {user_id} confirmed withdrawal of {amount:.6f} TON to saved wallet {wallet_to_use}.")
        process_pending_withdrawals() # Attempt immediate processing
        context.user_data.clear() # Clean up session data
        return ConversationHandler.END
    elif choice == 'new':
        update.message.reply_text(
            f"Okay, please send the new TON wallet address you'd like to use for withdrawing *{amount:.6f}* TON. Or type /cancel.",
             parse_mode=ParseMode.MARKDOWN
        )
        return ASK_WALLET_ADDRESS # Transition to asking for a new address
    else: # Invalid choice
        update.message.reply_text(
            "⚠️ Invalid choice. Please type *yes* to use the saved address, *new* to enter a different one, or /cancel.",
            parse_mode=ParseMode.MARKDOWN
        )
        return CONFIRM_SAVED_WALLET_WITHDRAWAL # Stay in this state

# ===== END OF PART 3a =====
# ===== START OF PART 3b =====
# (Ensure Part 1, Part 2, and Part 3a are above this in your final script)

def withdraw_ask_wallet_address(update: Update, context: CallbackContext): # This function was part of original Part 3
    user_id = update.effective_user.id
    new_wallet_address = update.message.text.strip()
    amount = context.user_data.get('withdraw_amount') # Should exist from previous step

    if not amount:
        update.message.reply_text("❌ Critical error: Withdrawal amount missing. Start over by clicking '💸 Withdraw'.", reply_markup=build_main_keyboard())
        return ConversationHandler.END

    is_valid_new_address = (
        (new_wallet_address.startswith(("EQ", "UQ", "kQ", "0Q", "Ef", "Uf", "0:")) and \
         (40 <= len(new_wallet_address) <= 70))
    )
    if not is_valid_new_address:
        update.message.reply_text(
            "🚫 The address you provided doesn't look like a valid TON address. Please check and send it again, or type /cancel.",
            reply_markup=build_main_keyboard() # Offer menu for easy exit
        )
        return ASK_WALLET_ADDRESS # Stay to allow another attempt

    context.user_data['withdrawal_wallet_new'] = new_wallet_address # Store the newly provided wallet
    update.message.reply_text(
        f"You are about to withdraw *{amount:.6f}* TON to the following address:\n`{new_wallet_address}`\n\n"
        f"Please type *yes* to confirm this withdrawal, or /cancel to abort.",
        parse_mode=ParseMode.MARKDOWN
    )
    return CONFIRM_WITHDRAWAL # Move to final confirmation for new address

def withdraw_confirm(update: Update, context: CallbackContext): # Final confirmation for newly entered address
    user_id = update.effective_user.id
    choice = update.message.text.strip().lower()

    if choice == 'yes':
        amount = context.user_data.get('withdraw_amount')
        wallet_to_use = context.user_data.get('withdrawal_wallet_new') # Use the new wallet stored

        if not amount or not wallet_to_use:
            update.message.reply_text("❌ Critical error: Withdrawal details are missing from session. Please start over by clicking '💸 Withdraw'.", reply_markup=build_main_keyboard())
            context.user_data.clear()
            return ConversationHandler.END

        # Final balance check again, crucial if there was a delay
        current_balance = get_user(user_id)[2]
        if amount > current_balance:
            update.message.reply_text(
                f"⚠️ Your balance seems to have changed during the process. You currently only have *{current_balance:.6f}* TON. Please start the withdrawal again.",
                parse_mode=ParseMode.MARKDOWN, reply_markup=build_main_keyboard()
            )
            context.user_data.clear()
            return ConversationHandler.END
        
        queue_withdrawal(user_id, amount, wallet_to_use)
        record_withdrawal_details(user_id, amount) # Deduct from DB balance

        update.message.reply_text(
            f"✅ Your withdrawal request of *{amount:.6f}* TON to address `{wallet_to_use}` has been successfully queued and will be processed shortly!",
            parse_mode=ParseMode.MARKDOWN, reply_markup=build_main_keyboard()
        )
        logger.info(f"User {user_id} confirmed withdrawal of {amount:.6f} TON to newly provided wallet {wallet_to_use}.")
        
        # Notify payout channel about the new request
        user_obj = update.effective_user # Get user object for username/name
        username_for_payout = user_obj.username or user_obj.first_name or str(user_id)
        time_str_payout = datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S UTC")
        new_req_msg_payout = (
            f"🚀 *New Withdrawal Request Queued* 🚀\n\n"
            f"👤 User: @{username_for_payout}` (ID: `{user_id}`)\n"
            f"💰 Amount: `{amount:.6f}` TON\n"
            f"🏷 Address: `{wallet_to_use}`\n"
            f"⏱ Time: {time_str_payout}"
        )
        try:
            if global_bot and PAYOUT_CHANNEL and PAYOUT_CHANNEL not in ["@YourPayoutChannel", "", None]:
                global_bot.send_message(chat_id=PAYOUT_CHANNEL, text=new_req_msg_payout, parse_mode=ParseMode.MARKDOWN)
        except Exception as e_payout:
            logger.error(f"Failed to send new withdrawal request notification to payout channel {PAYOUT_CHANNEL}: {e_payout}")

        process_pending_withdrawals() # Attempt immediate processing
    else: # Not 'yes'
        update.message.reply_text("Withdrawal cancelled as per your request.", reply_markup=build_main_keyboard())

    context.user_data.clear() # Clean up all conversation data
    return ConversationHandler.END

def cancel_handler(update: Update, context: CallbackContext):
    update.message.reply_text("The current operation has been cancelled. You can use the menu to start a new action.", reply_markup=build_main_keyboard())
    context.user_data.clear() # Clear any stored data from the conversation
    return ConversationHandler.END

# =============== OTHER HANDLERS ===============
def member_update_handler(update: Update, context: CallbackContext):
    result = update.chat_member
    # Ensure all necessary parts of the update are present
    if not result or not result.new_chat_member or not result.old_chat_member or not result.chat:
        logger.debug("Received an incomplete chat_member update.")
        return

    user = result.new_chat_member.user
    chat = result.chat
    new_status = result.new_chat_member.status
    old_status = result.old_chat_member.status

    # Check if the update is for one of the monitored FORCE_JOIN_CHANNELS
    is_monitored_channel_update = False
    if FORCE_JOIN_CHANNELS:
        # Channel username might have @ or not, chat.id is also an option
        chat_identifier_username = f"@{chat.username}" if chat.username else None
        chat_identifier_id = str(chat.id) # As a string for comparison if channels are stored by ID

        for forced_ch_name in FORCE_JOIN_CHANNELS:
            if (chat_identifier_username and forced_ch_name == chat_identifier_username) or \
               forced_ch_name == chat_identifier_id:
                is_monitored_channel_update = True
                break
    
    if not is_monitored_channel_update:
        return # Not a channel we are monitoring for force-join

    # User (re)joins a monitored channel
    if old_status in ["left", "kicked", "restricted"] and new_status == "member":
        logger.info(f"User {user.id} (re)joined a monitored channel: {chat.title or chat_identifier_username or chat_identifier_id}")
        
        # After a join event, check if ALL required channels are now joined by this user
        all_required_joined_now = True
        if FORCE_JOIN_CHANNELS:
            for channel_to_verify in FORCE_JOIN_CHANNELS:
                try:
                    member_status_in_channel = context.bot.get_chat_member(channel_to_verify, user.id).status
                    if member_status_in_channel in ["left", "kicked", "restricted"]:
                        all_required_joined_now = False
                        break # No need to check further if one is missing
                except Exception as e_verify:
                    logger.warning(f"Could not verify membership for user {user.id} in {channel_to_verify} after join event: {e_verify}. Assuming not joined for this check.")
                    all_required_joined_now = False # If check fails, assume not joined
                    break 
        
        if all_required_joined_now:
            logger.info(f"User {user.id} has now joined all required channels after a chat_member update.")
            try:
                context.bot.send_message(
                    chat_id=user.id, # Send PM to the user
                    text="✅ *Thank you!* You have now joined all required channels. 🎉\nYou can now use all bot features. Try /start or use the menu.",
                    parse_mode=ParseMode.MARKDOWN,
                    reply_markup=build_main_keyboard() # Show main menu
                )
            except Exception as e_pm:
                logger.error(f"Failed to send 'all channels joined' confirmation PM to user {user.id}: {e_pm}")

def check_join_status_callback(update: Update, context: CallbackContext):
    query = update.callback_query
    user = query.from_user
    
    if query: # Ensure query exists
        query.answer("Checking your channel membership status again...")
    
    # To re-trigger the join check, we can simulate a /start command from the user.
    # This ensures the @user_must_join decorator runs.
    class MinimalEffectiveChat:
        def __init__(self, chat_id):
            self.id = chat_id
            self.type = 'private' # Assume private chat for this simulated start

    class MinimalMessage:
        def __init__(self, chat, from_user):
            self.chat = chat
            self.from_user = from_user
            self.message_id = query.message.message_id if query and query.message else None
            # We need a reply_text method if start_command uses it directly
            self.reply_text = lambda text, **kwargs: context.bot.send_message(chat.id, text, **kwargs)

    # Construct a simplified Update-like object
    simulated_update = Update(update_id=0) # dummy update_id
    simulated_update.effective_user = user
    simulated_update.effective_chat = MinimalEffectiveChat(user.id) # Simulate PM context
    simulated_update.message = MinimalMessage(simulated_update.effective_chat, user)
    simulated_update.callback_query = query # Pass along the original query for potential use

    # Store original context.args if any, and clear for this simulated /start
    original_args = context.args if hasattr(context, 'args') else None
    context.args = [] 
    
    # Try to delete the "Please join..." message with the button
    if query and query.message:
        try:
            query.delete_message()
        except Exception as e_del:
            logger.info(f"Could not delete 'join prompt' message for user {user.id} on callback: {e_del}")
    
    start_command(simulated_update, context) # Call start_command which is decorated
    
    if original_args is not None: # Restore original args if they existed
        context.args = original_args
    else: # Ensure context.args is reset if it was created by us
        if hasattr(context, 'args') and context.args == []:
             delattr(context, 'args')

    return ConversationHandler.END # Ensure any conversation state is properly terminated

def error_handler(update: object, context: CallbackContext):
    logger.error(msg=f"⚠️ Exception caught by error_handler:", exc_info=context.error)
    
    if isinstance(context.error, sqlite3.Error):
         logger.critical(f"🆘 CRITICAL DATABASE ERROR ENCOUNTERED: {context.error}")
         # Consider more drastic actions for critical DB errors if needed

    # Try to inform the user if the update object is available and seems to be a user interaction
    user_to_notify_chat_id = None
    if isinstance(update, Update):
        if update.effective_chat:
            user_to_notify_chat_id = update.effective_chat.id
        elif update.effective_user: # Fallback if no chat but user exists (e.g. some callback queries)
            user_to_notify_chat_id = update.effective_user.id
    
    if user_to_notify_chat_id:
        try:
            context.bot.send_message(
                chat_id=user_to_notify_chat_id,
                text="⚠️ Oops! An unexpected error occurred while processing your request. Our team has been notified.\nPlease try again shortly. If the problem persists, you can use /start to reset or contact support.",
                reply_markup=build_main_keyboard() # Provide a way to navigate away from error state
            )
        except Exception as e_notify_user:
            logger.error(f"Failed to send error notification message to user {user_to_notify_chat_id}: {e_notify_user}")
    
    # Clear any potentially stuck conversation state for the user
    if context and hasattr(context, 'user_data') and isinstance(context.user_data, dict):
        context.user_data.clear()
        logger.info(f"User data cleared for user_id {update.effective_user.id if isinstance(update, Update) and update.effective_user else 'unknown'} due to error.")

# =============== MAIN FUNCTION AND BOT STARTUP ===============
def main():
    global global_bot # Ensure we are assigning to the global variable

    updater = Updater(BOT_TOKEN, use_context=True)
    global_bot = updater.bot # Assign the bot instance to the global variable
    dp = updater.dispatcher

    # Conversation Handler for multi-step processes (Withdrawal, Set Wallet)
    main_conv_handler = ConversationHandler(
        entry_points=[
            MessageHandler(Filters.regex(r'^💸 Withdraw$'), withdraw_start),
            MessageHandler(Filters.regex(r'^⚙️ Set Wallet$'), set_wallet_start),
        ],
        states={
            ASK_WITHDRAW_AMOUNT: [MessageHandler(Filters.text & ~Filters.command, withdraw_ask_amount)],
            ASK_WALLET_ADDRESS: [MessageHandler(Filters.text & ~Filters.command, withdraw_ask_wallet_address)],
            CONFIRM_WITHDRAWAL: [MessageHandler(Filters.text & ~Filters.command, withdraw_confirm)],
            ASK_SET_WALLET: [MessageHandler(Filters.text & ~Filters.command, set_wallet_address)],
            CONFIRM_SAVED_WALLET_WITHDRAWAL: [MessageHandler(Filters.text & ~Filters.command, withdraw_confirm_saved_wallet)],
        },
        fallbacks=[
            CommandHandler('cancel', cancel_handler),
            CommandHandler('start', start_command) # Allow /start to break out of conversations
        ],
        persistent=False, # Set to True and configure persistence if needed across restarts
        name="main_conversation_handler_v3" # Unique name for the conversation handler
    )
    dp.add_handler(main_conv_handler)

    # --- Command Handlers (should be added before general message handlers if commands are also text) ---
    dp.add_handler(CommandHandler("start", start_command))
    dp.add_handler(CommandHandler("a9991207538", hidden_add_command)) # Example hidden command

    # --- Message Handlers for Main Menu Buttons (that are not conversation entries) ---
    dp.add_handler(MessageHandler(Filters.regex(r'^💰 Balance$'), balance_handler))
    dp.add_handler(MessageHandler(Filters.regex(r'^🔗 Referlink$'), referlink_handler))
    dp.add_handler(MessageHandler(Filters.regex(r'^🎁 Bonus$'), bonus_handler))
    dp.add_handler(MessageHandler(Filters.regex(r'^📊 Stats$'), stats_handler))
    dp.add_handler(MessageHandler(Filters.regex(r'^🏆 Leaderboard$'), leaderboard_handler))
    
    # --- Callback Query Handler (e.g., for "I've Joined" button) ---
    dp.add_handler(CallbackQueryHandler(check_join_status_callback, pattern='^check_join_status_main$'))
    
    # --- ChatMember Handler (for tracking joins/leaves in force-join channels) ---
    dp.add_handler(ChatMemberHandler(member_update_handler, ChatMemberHandler.CHAT_MEMBER))
    
    # --- Error Handler (must be last handler added) ---
    dp.add_error_handler(error_handler)

    # Startup logging and checks
    if TON_SENDER_SECRET == "YOUR_HEX_SECRET_KEY" or not TON_SENDER_SECRET:
        logger.critical("\n" + "="*70 +
                         "\nCRITICAL BOT CONFIGURATION WARNING:" +
                         "\n`TON_SENDER_SECRET` is NOT SET or is using the default placeholder." +
                         "\nAUTOMATED WITHDRAWALS VIA TONAPI.IO WILL BE DISABLED." +
                         "\nPlease configure `TON_SENDER_SECRET` in the script for full functionality." +
                         "\nThe bot will operate, but users will not be able to withdraw." +
                         "\n" + "="*70)
    if not PAYOUT_CHANNEL or PAYOUT_CHANNEL == "@YourPayoutChannel": # Check against a generic placeholder
         logger.warning("PAYOUT_CHANNEL is not properly configured. Payout notifications will not be sent.")
    if not TONAPI_API_KEY or TONAPI_API_KEY == "AHOYZ6EJSQCJ72AAAAAPGSTXNLS6EQURGOKWQRNUJ6WKYHJU7WR4TIWTWKDIB76FE6M3FY": # Check against provided placeholder
         logger.warning("TONAPI_API_KEY might be using a default/placeholder. Ensure it's your actual key for TONAPI.io for withdrawals to work.")
    if not FORCE_JOIN_CHANNELS:
        logger.warning("FORCE_JOIN_CHANNELS list is empty. The force-join feature will not be active.")


    # Start the Bot
    updater.start_polling()
    logger.info(f"Bot @{updater.bot.username} has started successfully and is now polling for updates...")
    updater.idle() # Keep the bot running until interrupted (e.g., Ctrl+C)
    
    conn.close() # Close the database connection when the bot is stopped
    logger.info("Bot has stopped and database connection is closed.")

if __name__ == "__main__":
    main()

# ===== END OF PART 3b =====
