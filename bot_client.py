import os
from aiogram import Bot

bot = Bot(token='7524894893:AAH914INGndjbSIGGURvYo7B1PDCBvAPth0')


async def send_message(chat_id: int, text: str):
    """Отправка сообщения пользователю"""
    try:
        await bot.send_message(chat_id, text)
    except Exception as e:
        print(f"Ошибка отправки сообщения: {e}")
