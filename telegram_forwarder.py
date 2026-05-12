from telethon import TelegramClient, events, sync

import os
from dotenv import load_dotenv

# .env dosyasını yükle
load_dotenv()

# API bilgileri (Telegram'dan alın: https://my.telegram.org/auth)
API_ID_ENV = os.getenv('API_ID')
API_HASH = os.getenv('API_HASH')
PHONE = os.getenv('PHONE')
SOURCE_CHAT_ID = os.getenv('SOURCE_CHAT_ID')
DEST_CHAT_ID = os.getenv('DEST_CHAT_ID')

if not all([API_ID_ENV, API_HASH, PHONE, SOURCE_CHAT_ID, DEST_CHAT_ID]):
    print("HATA: .env dosyasındaki eksik bilgileri doldurmalısın!")
    print("Lütfen API_ID, API_HASH, PHONE, SOURCE_CHAT_ID ve DEST_CHAT_ID değerlerini kontrol et.")
    exit(1)

API_ID = int(API_ID_ENV)

FILTER_TYPE = os.getenv('FILTER_TYPE', 'none')  # 'whitelist', 'blacklist', 'none'
FILTER_WORDS_STR = os.getenv('FILTER_WORDS', '')
FILTER_WORDS = [word.strip() for word in FILTER_WORDS_STR.split(',') if word.strip()]

client = TelegramClient('session_name', API_ID, API_HASH)

@client.on(events.NewMessage(chats=[int(SOURCE_CHAT_ID)]))
async def forward(event):
    message = event.message
    if not message.text or message.out:
        return
    
    text = message.text.lower()
    
    if FILTER_TYPE == 'whitelist':
        if not any(word.lower() in text for word in FILTER_WORDS):
            return
    elif FILTER_TYPE == 'blacklist':
        if any(word.lower() in text for word in FILTER_WORDS):
            return
    
    await client.send_message(int(DEST_CHAT_ID), message.text)

async def main():
    await client.start(phone=PHONE)
    print("Client started. Listening for messages...")
    await client.run_until_disconnected()

if __name__ == '__main__':
    client.loop.run_until_complete(main())
