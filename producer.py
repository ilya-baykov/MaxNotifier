import json
import asyncio

# --- Синхронный Producer ---
from pika import ConnectionParameters, BlockingConnection

class Producer:
    def __init__(self, host='localhost', port=5672, queue_name='max_notifications'):
        self.connection_params = ConnectionParameters(host=host, port=port)
        self.queue_name = queue_name

    def send(self, message: dict):
        """Отправка сообщения в очередь (синхронно)"""
        with BlockingConnection(self.connection_params) as connection:
            with connection.channel() as channel:
                channel.queue_declare(queue=self.queue_name, durable=True)
                channel.basic_publish(
                    exchange='',
                    routing_key=self.queue_name,
                    body=json.dumps(message)
                )
                print(f'[SYNC] Сообщение отправлено в очередь {self.queue_name}')


# --- Асинхронный AsyncProducer ---
import aio_pika

class AsyncProducer:
    def __init__(self, url='amqp://guest:guest@localhost/', queue_name='max_notifications'):
        self.url = url
        self.queue_name = queue_name

    async def send(self, message: dict):
        """Отправка сообщения в очередь (асинхронно)"""
        connection = await aio_pika.connect_robust(self.url)
        async with connection:
            channel = await connection.channel()
            queue = await channel.declare_queue(self.queue_name, durable=True)
            await channel.default_exchange.publish(
                aio_pika.Message(body=json.dumps(message).encode()),
                routing_key=self.queue_name
            )
            print(f'[ASYNC] Сообщение отправлено в очередь {self.queue_name}')


# --- Пример использования ---
if __name__ == '__main__':
    message = {"chat_id": 793353522, "message_text": "Привет, это тестовое сообщение!"}

    # # Синхронный Producer
    # producer = Producer()
    # producer.send(message)

    # Асинхронный Producer
    async def async_main():
        async_producer = AsyncProducer()
        await async_producer.send(message)

    asyncio.run(async_main())
