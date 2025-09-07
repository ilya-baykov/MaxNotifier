import json
import aio_pika
from typing import Dict, Any, Optional

from bot_client import MaxBotClient
from logger_settings import logger
from config import DEFAULT_BROKER_URL, DEFAULT_QUEUE_NAME


class MaxWorker:
    """
    Воркер для обработки сообщений из очереди RabbitMQ
    и пересылки их в мессенджер MAX через MaxBotClient.
    """

    def __init__(self, bot_token: str,
                 broker_url: str = DEFAULT_BROKER_URL,
                 queue_name: str = DEFAULT_QUEUE_NAME) -> None:
        """
        Инициализация воркера.

        :param bot_token: токен бота MAX
        :param broker_url: строка подключения к RabbitMQ
        :param queue_name: имя очереди для уведомлений
        """
        self._bot_client = MaxBotClient(bot_token)  # подключаем бота один раз
        self._broker_url: str = broker_url
        self._queue_name: str = queue_name
        self._connection: Optional[aio_pika.RobustConnection] = None
        logger.info("MaxWorker создан с брокером %s и очередью '%s'", broker_url, queue_name)

    async def _connect_rabbit(self) -> None:
        """Подключение к RabbitMQ и создание канала/очереди."""
        if self._connection is None:
            logger.info("Подключение к RabbitMQ: %s", self._broker_url)
            self._connection = await aio_pika.connect_robust(self._broker_url)
            channel = await self._connection.channel()
            await channel.declare_queue(self._queue_name, durable=True)
            self._channel = channel
            logger.info("Очередь '%s' готова.", self._queue_name)

    async def process_message(self, message: aio_pika.IncomingMessage) -> None:
        """
        Обработчик одного сообщения из очереди.

        :param message: объект сообщения RabbitMQ
        """
        async with message.process():  # подтверждаем обработку
            try:
                data: Dict[str, Any] = json.loads(message.body.decode())
                chat_id: str = data["chat_id"]
                text: str = data["text"]
            except Exception as e:
                logger.error("Ошибка при разборе сообщения: %s", e)
                return

            await self._bot_client.send_message(chat_id, text)

    async def run(self) -> aio_pika.RobustConnection:
        """
        Запуск воркера: подключение к RabbitMQ и подписка на очередь.

        :return: объект соединения с RabbitMQ
        """
        await self._connect_rabbit()
        queue = await self._channel.declare_queue(self._queue_name, durable=True)
        await queue.consume(self.process_message)
        logger.info("Worker запущен. Ожидание сообщений в очереди '%s'...", self._queue_name)
        return self._connection
