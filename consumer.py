import asyncio
import json
import aio_pika
from bot_client import send_message

RABBITMQ_URL = "amqp://guest:guest@localhost/"  # или другой URL
QUEUE_NAME = "max_notifications"


async def process_message(message: aio_pika.IncomingMessage):
    async with message.process():
        try:
            payload = json.loads(message.body.decode())
            print(f"Получено сообщение: {payload}")
            chat_id = payload.get("chat_id")
            text = payload.get("message_text")
            if chat_id and text:
                await send_message(chat_id, text)
        except Exception as e:
            print(f"Ошибка обработки сообщения: {e}")


async def main():
    # Подключение к RabbitMQ
    connection = await aio_pika.connect_robust(RABBITMQ_URL)
    async with connection:
        channel = await connection.channel()
        # Опционально: создаем очередь, если её нет
        queue = await channel.declare_queue(QUEUE_NAME, durable=True)

        print("Жду сообщений...")
        # Подписка на очередь
        await queue.consume(process_message)

        # Ждем вечность
        await asyncio.Future()  # run forever


if __name__ == "__main__":
    asyncio.run(main())
