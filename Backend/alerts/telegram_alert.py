# alerts/telegram_alert.py

import requests

class TelegramAlert:
    def __init__(self, token, chat_id):
        self.token = token
        self.chat_id = chat_id

    def send_alert(self, message):
        url = f"https://api.telegram.org/bot{self.token}/sendMessage"
        payload = {
            'chat_id': self.chat_id,
            'text': message
        }
        try:
            res = requests.post(url, data=payload)
            if res.status_code != 200:
                print(f"❗ Telegram Error: {res.text}")
        except Exception as e:
            print(f"❌ Telegram exception: {e}")
