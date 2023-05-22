# -*- coding: utf-8 -*-
# pylint: disable=C0111,C0103,R0205

import json
import pika
from pika.exchange_type import ExchangeType

print('pika version: %s' % pika.__version__)

try:
    connection = pika.BlockingConnection(
        pika.ConnectionParameters(host='localhost'))
    main_channel = connection.channel()
    main_channel.exchange_declare(exchange='asyncio.exchange', exchange_type=ExchangeType.direct)

    for i in range(0, 100):
        msg = {
            'route.key.ticker': {
                'data': {
                    'params': {
                        'condition': {
                            'ticker': i
                        }
                    }
                }
            }
        }
        main_channel.basic_publish(
            exchange='asyncio.exchange',
            routing_key='asyncio.route.key.ticker',
            body=json.dumps(msg),
            properties=pika.BasicProperties(content_type='application/json'))
        print('send ticker %s' % i)

    connection.close()
# Don't recover if connection was closed by broker
except pika.exceptions.ConnectionClosedByBroker as e:
    print('exception: ', e)
# Don't recover on channel errors
except pika.exceptions.AMQPChannelError as e:
    print('exception: ', e)
# Recover on all other connection errors
except pika.exceptions.AMQPConnectionError as e:
    print('exception: ', e)


