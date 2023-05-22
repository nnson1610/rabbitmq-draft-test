import pika
import requests

print('pika version: %s' % pika.__version__)

def hello():
    print('Hello world')

def callback(_ch, _method, _properties, body):
    x = requests.get('https://w3schools.com')
    print(x.status_code)
    print('got ticker %s, gonna bind it...' % body)
    channel.basic_ack(delivery_tag=_method.delivery_tag)

try: 
    credentials = pika.PlainCredentials('guest', 'guest')
    parameters = pika.ConnectionParameters(
        'localhost', credentials=credentials, heartbeat=5)
    connection = pika.BlockingConnection(parameters)
    channel = connection.channel()

    channel.queue_declare(queue="simple")
    channel.queue_bind(
        exchange='simple.exchange', queue="simple", routing_key='route.key.ticker')
    channel.basic_qos(prefetch_count=100)

    # Note: consuming with automatic acknowledgements has its risks
    #       and used here for simplicity.
    #       See https://www.rabbitmq.com/confirms.html.
    channel.basic_consume(queue='simple', on_message_callback=callback, auto_ack=False)
    channel.start_consuming()
    connection.close()

# Don't recover if connection was closed by broker
except pika.exceptions.ConnectionClosedByBroker as e:
    print('exception: ', e)
    channel.stop_consuming()
# Don't recover on channel errors
except pika.exceptions.AMQPChannelError as e:
    print('exception: ', e)
    channel.stop_consuming()
# Recover on all other connection errors
except pika.exceptions.AMQPConnectionError as e:
    print('exception: ', e)
    channel.stop_consuming()







