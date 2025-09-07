from aiomax import Bot
from typing import Optional

from config.logger_settings import logger


class MaxBotClient:
    """
    Клиент для работы с ботом MAX.
    Один объект создаётся при запуске программы.
    """

    def __init__(self, token: str) -> None:
        """
        Инициализация бота MAX.

        :param token: токен бота MAX
        """
        self._token: str = token
        self._bot: Optional[Bot] = None
        self._initialize_bot()

    def _initialize_bot(self) -> None:
        """Создаёт объект бота."""
        if self._bot is None:
            self._bot = Bot(token=self._token)
            logger.info("Бот MAX инициализирован.")

    async def send_message(self, chat_id: str, text: str) -> None:
        """
        Отправка сообщения через бота MAX.

        :param chat_id: идентификатор чата
        :param text: текст сообщения
        """
        if not self._bot:
            logger.error("Бот не инициализирован")
            return

        try:
            await self._bot.send_message(chat_id, text)
            logger.info("Сообщение отправлено в chat_id %s", chat_id)
        except Exception as e:
            logger.error("Ошибка отправки сообщения через бота MAX: %s", e)
