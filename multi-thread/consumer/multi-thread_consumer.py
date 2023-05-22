# -*- coding: utf-8 -*-
# pylint: disable=C0111,C0103,R0205

import functools
import threading
import pika
import requests

def ack_message(ch, delivery_tag):
    """Note that `ch` must be the same pika channel instance via which
    the message being ACKed was retrieved (AMQP protocol constraint).
    """
    if ch.is_open:
        print("ack_delivery_tag: ", delivery_tag)
        ch.basic_ack(delivery_tag)
    else:
        # Channel is already closed, so we can't ACK this message;
        # log and/or do something that makes sense for your app in this case.
        pass


def do_work(ch, delivery_tag, body):
    thread_id = threading.get_ident()
    print('Thread id: ', thread_id, "delivery_tag: ", delivery_tag, "Body: ",body)
    x = requests.get('https://w3schools.com')
    print('Thread id: ', thread_id, "delivery_tag: ", delivery_tag, "Body: ",body,  " Status: ", x.status_code)

    # Sleeping to simulate 10 seconds of work
    # time.sleep(10)
    cb = functools.partial(ack_message, ch, delivery_tag)

    # SHOULD USE the line below to make sure thread-safe
    ch.connection.add_callback_threadsafe(cb)

    # DO NOT USE line below, will cause stuck if we consume bunch of messages cause it's not thread-safe
    # ack_message(ch, delivery_tag) 


def on_message(ch, method_frame, _header_frame, body, args):
    thrds = args
    delivery_tag = method_frame.delivery_tag
    t = threading.Thread(target=do_work, args=(ch, delivery_tag, body))
    t.start()
    thrds.append(t)


credentials = pika.PlainCredentials('guest', 'guest')
# Note: sending a short heartbeat to prove that heartbeats are still
# sent even though the worker simulates long-running work
parameters = pika.ConnectionParameters(
    'localhost', credentials=credentials, heartbeat=5)
connection = pika.BlockingConnection(parameters)

channel = connection.channel()

channel.queue_declare(queue="thread_basic")
channel.queue_bind(
    queue="thread_basic", exchange="thread.exchange", routing_key="thread.route.key.ticker")
# Note: prefetch is set to 1 here as an example only and to keep the number of threads created
# to a reasonable amount. In production you will want to test with different prefetch values
# to find which one provides the best performance and usability for your solution
channel.basic_qos(prefetch_count=100)

threads = []
on_message_callback = functools.partial(on_message, args=(threads))
channel.basic_consume(on_message_callback=on_message_callback, queue='thread_basic')

try:
    channel.start_consuming()
except KeyboardInterrupt:
    channel.stop_consuming()

# Wait for all to complete
for thread in threads:
    thread.join()

connection.close()