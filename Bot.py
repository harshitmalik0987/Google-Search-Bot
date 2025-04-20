import random
import logging
from telethon import TelegramClient, events
import aiohttp
import asyncio

# ─── CONFIG ─────────────────────────────────────────────────────────────────────
api_id = 22815674
api_hash = '3aa83fb0fe83164b9fee00a1d0b31e5f'
phone_number = '+919350050226'
CHANNEL_USERNAME = 'govt_jobnotification'  # Updated channel name

N1PANEL_URL = 'https://n1panel.com/api/v2'
API_KEY = '93600468f93f081f51123815b5b9f409'
# ────────────────────────────────────────────────────────────────────────────────

# Setup logging
logging.basicConfig(
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    level=logging.INFO
)
logger = logging.getLogger(__name__)

client = TelegramClient('govt_notification_session', api_id, api_hash)

async def call_n1panel(session, service_id, quantity, link):
    params = {
        'action': 'add',
        'service': service_id,
        'link': link,
        'quantity': quantity,
        'key': API_KEY
    }
    try:
        async with session.post(N1PANEL_URL, data=params, timeout=10) as response:
            response_text = await response.text()
            try:
                response_json = await response.json()
                if response_json.get('success', False):
                    logger.info(f"Service {service_id} success: {response_json}")
                    return True
                else:
                    logger.error(f"Service {service_id} error: {response_json}")
                    return False
            except ValueError:
                logger.error(f"Service {service_id} invalid JSON response: {response_text}")
                return False
    except Exception as e:
        logger.error(f"Service {service_id} fatal error: {str(e)}")
        return False

@client.on(events.NewMessage(chats=CHANNEL_USERNAME))
async def handler(event):
    try:
        message = event.message
        link = f"https://t.me/{CHANNEL_USERNAME}/{message.id}"
        logger.info(f"New post detected: {link}")

        qty1 = random.randint(200, 250)
        qty2 = random.randint(10, 15)

        async with aiohttp.ClientSession() as session:
            await asyncio.gather(
                call_n1panel(session, 3183, qty1, link),
                call_n1panel(session, 3232, qty2, link)
            )
    except Exception as e:
        logger.error(f"Handler error: {str(e)}")

async def main():
    await client.start(phone=phone_number)
    logger.info("Client started. Listening for new posts...")
    await client.run_until_disconnected()

if __name__ == '__main__':
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.info("Client stopped by user")
    except Exception as e:
        logger.error(f"Fatal error: {str(e)}")
