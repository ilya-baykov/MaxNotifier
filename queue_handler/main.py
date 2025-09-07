import os

from queue_handler.max_bot_client import MaxBotClient
from queue_handler.queue_handler import QueueHandler

if __name__ == '__main__':
    BOT_TOKEN = os.getenv('BOT_TOKEN')
    BROKER_URL = os.getenv('BROKER_URL')
    QUEUE_NAME = os.getenv('QUEUE_NAME')

    bot_client = MaxBotClient(BOT_TOKEN)
    queue_handler = QueueHandler(BROKER_URL, QUEUE_NAME, bot_client)
    queue_handler.start_consuming()
