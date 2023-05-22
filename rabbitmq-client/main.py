from rmqPublisher import RabbitMQPublisher
from rmqConsumer import RabbitMQConsumer
from rmqClient import RabbitMQClient
from pika.exchange_type import ExchangeType

if __name__ == '__main__':
    RABBITMQ_HOST = 'localhost'
    RABBITMQ_USER = 'guest'
    RABBITMQ_PWD = 'guest'
    RABBITMQ_PORT = '5672'

    FIRST_EXCHANGE_NAME = 'first.exchange'
    SECOND_EXCHANGE_NAME = 'second.exchange'
    FIRST_QUEUE_NAME = 'first.queue'  
    SECOND_QUEUE_NAME = 'second.queue'  

    EXCHANGES = [
        {
            'name': FIRST_EXCHANGE_NAME,
            'type': ExchangeType.fanout,
        }
    ]

    QUEUES =  [
        {
            'name': FIRST_QUEUE_NAME,
            'exchange': FIRST_EXCHANGE_NAME,
            'routing_key': 'first.route'
        },
        {
            'name': SECOND_QUEUE_NAME,
            'exchange': FIRST_EXCHANGE_NAME,
            'routing_key': 'second.route'
        }
    ]

    try:
        rmqClient = RabbitMQClient(RABBITMQ_HOST, RABBITMQ_USER, RABBITMQ_PWD, RABBITMQ_PORT)

        # Declare a queue
        rmqClient.declare_exchanges(EXCHANGES)
        rmqClient.declare_queues(QUEUES)

        # Send a message
        rmqClient.send_message(exchange=FIRST_EXCHANGE_NAME, routing_key="", body=b'Hello World!')
        # rmqClient.consume_messages(FIRST_QUEUE_NAME)

        # Close connections.
        rmqClient.close()
    except KeyboardInterrupt:
        print('Interrupted')
