import asyncio
import os

from logger_settings import logger
from worker import MaxWorker


async def main():
    worker = MaxWorker(bot_token=os.getenv("BOT_TOKEN"), broker_url=os.getenv("BROKER_URL"))
    connection = await worker.run()
    try:
        await asyncio.Future()  # keep-alive
    finally:
        if connection:
            await connection.close()
            logger.info("Соединение с RabbitMQ закрыто.")


if __name__ == "__main__":
    asyncio.run(main())
