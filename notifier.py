import json
import pika
from typing import Optional

from config import DEFAULT_BROKER_URL, DEFAULT_QUEUE_NAME
from logger_settings import logger


class Notifier:
    """
    Паблишер уведомлений в очередь RabbitMQ для MAX.

    Используется внутри приложения для отправки сообщений в MAX через очередь.
    """

    def __init__(self, broker_url: str = DEFAULT_BROKER_URL, queue_name: str = DEFAULT_QUEUE_NAME) -> None:
        """
        Инициализация Notifier.

        :param broker_url: строка подключения к RabbitMQ
        :param queue_name: имя очереди для уведомлений
        """
        self._broker_url: str = broker_url
        self._queue_name: str = queue_name
        self._connection: Optional[pika.BlockingConnection] = None
        self._channel: Optional[pika.adapters.blocking_connection.BlockingChannel] = None
        logger.info("Notifier создан с брокером %s и очередью '%s'", broker_url, queue_name)

    def _connect(self) -> None:
        """Создаёт соединение и канал, если их ещё нет (ленивая инициализация)."""
        if self._connection is None or self._channel is None:
            params = pika.URLParameters(self._broker_url)
            self._connection = pika.BlockingConnection(params)
            self._channel = self._connection.channel()
            self._channel.queue_declare(queue=self._queue_name, durable=True)

    def send_message(self, chat_id: str, text: str) -> None:
        """
        Отправка сообщения в очередь RabbitMQ.

        :param chat_id: идентификатор чата в MAX
        :param text: текст сообщения
        """
        if not chat_id or not text:
            logger.error("chat_id и text обязательны для отправки сообщения")
            raise ValueError("chat_id и text обязательны")

        self._connect()  # лениво подключаемся, если ещё нет соединения

        message = {"chat_id": chat_id, "text": text}
        self._channel.basic_publish(
            exchange="",  # default exchange
            routing_key=self._queue_name,
            body=json.dumps(message),
            properties=pika.BasicProperties(delivery_mode=2),  # persistent
        )
        logger.info("Сообщение отправлено в очередь '%s' для chat_id %s", self._queue_name, chat_id)

    def close(self) -> None:
        """Закрывает соединение с RabbitMQ, если оно открыто."""
        if self._connection and self._connection.is_open:
            self._connection.close()
            self._connection = None
            self._channel = None
            logger.info("Соединение с RabbitMQ закрыто.")

    # Контекстный менеджер
    def __enter__(self) -> "Notifier":
        self._connect()
        return self

    def __exit__(self, exc_type, exc_value, traceback) -> None:
        self.close()
