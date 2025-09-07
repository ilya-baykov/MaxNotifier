# import asyncio
# import json
#
# from pika import ConnectionParameters, BlockingConnection, callback
#
# from bot_client import send_message
#
# connection_params = ConnectionParameters(
#     host='localhost',
#     port=5672
# )
#
#
# def process_message(channel, method, properties, body):
#     """Обработчик сообщений из RabbitMQ"""
#     try:
#         message = json.loads(body.decode())
#         print(f"Получено сообщение: {message}")
#         chat_id = message.get("chat_id")
#         text = message.get("message_text")
#         if chat_id and text:
#             asyncio.create_task(send_message(chat_id, text))
#     except Exception as e:
#         print(f"Ошибка обработки сообщения: {e}")
#     finally:
#         channel.basic_ack(delivery_tag=method.delivery_tag)
#
#
# def main():
#     with BlockingConnection(connection_params) as connection:
#         with connection.channel() as channel:
#             queue_name = 'max_notifications'
#
#             channel.queue_declare(queue=queue_name, durable=True)  # Опционально: убедимся, что очередь существует
#             channel.basic_consume(queue=queue_name, on_message_callback=process_message)
#             print(f"Жду сообщения")
#             channel.start_consuming()
#
#
# if __name__ == '__main__':
#     main()
import asyncio
import json
import aio_pika
from bot_client import send_message

RABBITMQ_URL = "amqp://guest:guest@localhost/"
QUEUE_NAME = "max_notifications"


async def process_message(message: aio_pika.IncomingMessage):
    async with message.process():
        try:
            msg = json.loads(message.body.decode())
            print(f"Получено сообщение: {msg}")
            chat_id = msg.get("chat_id")
            text = msg.get("message_text")
            if chat_id and text:
                await send_message(chat_id, text)
        except Exception as e:
            print(f"Ошибка обработки: {e}")


async def main():
    # Подключаемся к RabbitMQ
    connection = await aio_pika.connect_robust(RABBITMQ_URL)
    async with connection:
        channel = await connection.channel()
        queue = await channel.declare_queue(QUEUE_NAME, durable=True)
        print("Жду сообщений из очереди...")

        async with queue.iterator() as queue_iter:
            async for message in queue_iter:
                await process_message(message)


if __name__ == "__main__":
    asyncio.run(main())
