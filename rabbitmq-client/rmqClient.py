import pika

class RabbitMQClient:
    def __init__(self, rabbitmq_host, rabbitmq_user, rabbitmq_password, rabbitmq_port):
        self.connect(rabbitmq_host, rabbitmq_user, rabbitmq_password, rabbitmq_port)

    def connect(self, rabbitmq_host, rabbitmq_user, rabbitmq_password, rabbitmq_port):
        try:
            url = f"amqp://{rabbitmq_user}:{rabbitmq_password}@{rabbitmq_host}:{rabbitmq_port}"
            parameters = pika.URLParameters(url)

            self.connection = pika.BlockingConnection(parameters)
            self.channel = self.connection.channel()
            self.channel.basic_qos(prefetch_count=1) # TODO move to config

        # Don't recover if connection was closed by broker
        except pika.exceptions.ConnectionClosedByBroker as e:
            print('exception: ', e)
        # Don't recover on channel errors
        except pika.exceptions.AMQPChannelError as e:
            print('exception: ', e)
        # Recover on all other connection errors
        except pika.exceptions.AMQPConnectionError as e:
            print('exception: ', e)

    # PRODUCER 
    def declare_exchanges(self, exchanges):
        for item in exchanges:
            print(f"Trying to declare exchange({item['name']}) with type({item['name']})...")
            self.channel.exchange_declare(exchange=item['name'], exchange_type=item['type'])

    def declare_queues(self, queues):
        for item in queues:
            self.channel.queue_declare(queue=item['name'])
            self.channel.queue_bind(
                queue=item['name'], exchange=item['exchange'], routing_key=item['routing_key'])
            print(f"Trying to declare queue({item['name']}) with exchange({item['exchange']}) and routing_key({item['routing_key']})...")

    def send_message(self, exchange, routing_key, body):
        channel = self.connection.channel()
        channel.basic_publish(exchange=exchange,
                              routing_key=routing_key,
                              body=body)
        print(f"Sent message. Exchange: {exchange}, Routing Key: {routing_key}, Body: {body}")

    # CONSUMER 
    def get_message(self, queue):
        method_frame, header_frame, body = self.channel.basic_get(queue)
        if method_frame:
            print(method_frame, header_frame, body)
            self.channel.basic_ack(method_frame.delivery_tag)
            return method_frame, header_frame, body
        else:
            print('No message returned')

    def consume_messages(self, queue):
        def callback(ch, method, properties, body):
            print(" [x] Received %r" % body)

        self.channel.basic_consume(queue=queue, on_message_callback=callback, auto_ack=True)

        print(' [*] Waiting for messages. To exit press CTRL+C')
        try:
            self.channel.start_consuming()
        except KeyboardInterrupt:
            self.channel.stop_consuming()

    def close(self):
        self.channel.close()
        self.connection.close()