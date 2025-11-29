# utils/telegram_alert.py

import requests
import config

def send_telegram_message(message):
    url = f"https://api.telegram.org/bot{config.TELEGRAM_TOKEN}/sendMessage"
    payload = {
        "chat_id": config.TELEGRAM_CHAT_ID,
        "text": message
    }

    try:
        response = requests.post(url, json=payload)
        if response.status_code != 200:
            print(f"⚠️ Telegram Error: {response.text}")
    except Exception as e:
        print(f"❌ Exception sending Telegram message: {e}")
