from rmqClient import RabbitMQClient
from pika.exchange_type import ExchangeType

class RabbitMQPublisher(RabbitMQClient):
    def declare_exchanges(self, name, type):
        print(f"Trying to declare exchange({name}) with type({type})...")
        self.channel.exchange_declare(exchange=name, exchange_type=type)

    def declare_queue(self, queue_name):
        print(f"Trying to declare queue({queue_name})...")
        self.channel.queue_declare(queue=queue_name)

    def send_message(self, exchange, routing_key, body):
        channel = self.connection.channel()
        channel.basic_publish(exchange=exchange,
                              routing_key=routing_key,
                              body=body)
        print(f"Sent message. Exchange: {exchange}, Routing Key: {routing_key}, Body: {body}")

    def close(self):
        self.channel.close()
        self.connection.close()