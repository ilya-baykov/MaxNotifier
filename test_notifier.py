import asyncio
from notifier.notifier import Notifier

BROKER_URL = 'amqp://guest:guest@localhost:5672/'
QUEUE_NAME = 'max_notifications'


async def main():
    notifier_manager = Notifier(BROKER_URL, QUEUE_NAME)
    async with notifier_manager as notifier:
        await notifier.send_message("123456789", "Тестовое сообщение")
        await notifier.send_message("987654321", "Ещё одно сообщение")


asyncio.run(main())
