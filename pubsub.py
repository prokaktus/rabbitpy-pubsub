#!/usr/bin/env python3
import rabbitpy

class RabbitWrapper:
    def __init__(self):
        self.connection = rabbitpy.Connection()
        self.channel = self.connection.channel()
        self.exchange = rabbitpy.FanoutExchange(self.channel,
                                                'fanout_test', durable=True)
        self.exchange.declare()
        
    def __enter__(self):
        return self

    def __exit__(self, type, value, traceback):
        self.channel.close()
        self.connection.close()


class Publisher(RabbitWrapper):
    def __init__(self):
        super().__init__()
        
    def publish(self, blob):
        message = rabbitpy.Message(self.channel,
                                   blob,
                                   {'content_type':
                                    'application/octet-stream'})
        message.publish(self.exchange, '')

class Subscriber(RabbitWrapper):
    def __init__(self, queue):
        super().__init__()
        self.queue = rabbitpy.Queue(self.channel, queue)
        self.queue.declare()
        self.queue.bind(self.exchange)

    def get(self):
        for message in self.queue.consume():
            yield message

    def ack(self, ack_message):
        ack_message.ack()

    def __exit__(self, type, value, traceback):
        self.queue.delete()
        super().__exit__(type, value, traceback)
