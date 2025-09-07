import json

from pika import ConnectionParameters, BlockingConnection

connection_params = ConnectionParameters(
    host='localhost',
    port=5672
)
message = {
    "chat_id": 793353522,  # твой chat_id
    "message_text": "Привет, это тестовое сообщение!"
}


def main():
    with BlockingConnection(connection_params) as connection:
        with connection.channel() as channel:
            queue_name = 'max_notifications'

            channel.queue_declare(queue=queue_name, durable=True)  # Опционально: убедимся, что очередь существует

            # Отправка сообщения в очередь
            channel.basic_publish(
                exchange='',  # пустой exchange → default
                routing_key=queue_name,  # имя очереди
                body=json.dumps(message)
            )
            print(f'Сообщение отправлено в очередь {queue_name}')


if __name__ == '__main__':
    main()
