import asyncio
import json
from typing import Optional, Any

import pika
from pika.adapters.blocking_connection import BlockingChannel

from config.logger_settings import logger
from max_bot_client import MaxBotClient


class QueueHandler:
    """Обработчик очереди RabbitMQ, который пересылает сообщения в MAX бот."""

    def __init__(self, broker_url: str, queue_name: str, bot: MaxBotClient) -> None:
        self._broker_url = broker_url
        self._queue_name = queue_name
        self._bot = bot
        self._connection: Optional[pika.BlockingConnection] = None
        self._channel: Optional[BlockingChannel] = None

    def _connect(self) -> None:
        """Создаёт соединение и канал с RabbitMQ."""
        if self._connection is None or self._channel is None:
            params = pika.URLParameters(self._broker_url)
            self._connection = pika.BlockingConnection(params)
            self._channel = self._connection.channel()
            self._channel.queue_declare(queue=self._queue_name, durable=True)
            logger.info("Подключение к очереди '%s' установлено.", self._queue_name)

    def _callback(self, ch: BlockingChannel, method: Any, properties: Any, body: bytes) -> None:
        """Callback для обработки каждого сообщения из очереди."""
        try:
            message: dict[str, str] = json.loads(body)
            chat_id = message.get("chat_id")
            text = message.get("text")
            if chat_id and text:
                # Используем asyncio для вызова асинхронного метода
                asyncio.run(self._bot.send_message(chat_id, text))
            else:
                logger.error("Неверное сообщение: %s", message)
        except Exception as e:
            logger.error("Ошибка при обработке сообщения: %s", e)
        finally:
            if ch and method:
                ch.basic_ack(delivery_tag=method.delivery_tag)

    def start_consuming(self) -> None:
        """Запуск прослушивания очереди."""
        self._connect()
        if self._channel is None:
            raise RuntimeError("Канал RabbitMQ не инициализирован")

        self._channel.basic_qos(prefetch_count=1)  # Обработка по одному сообщению
        self._channel.basic_consume(queue=self._queue_name, on_message_callback=self._callback)
        logger.info("Начало прослушивания очереди '%s'...", self._queue_name)
        try:
            self._channel.start_consuming()
        except KeyboardInterrupt:
            logger.info("Остановка прослушивания очереди по сигналу KeyboardInterrupt")
        finally:
            self.close()

    def close(self) -> None:
        """Закрывает соединение."""
        if self._connection and self._connection.is_open:
            self._connection.close()
            logger.info("Соединение с RabbitMQ закрыто.")