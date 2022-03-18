import json

import pika

from xprint import xprint


class ShoppingEventProducer:

    def __init__(self):
        # Do not edit the init method.
        # Set the variables appropriately in the methods below.
        self.connection = None
        self.channel = None

    def initialize_rabbitmq(self):
        # To implement - Initialize the RabbitMq connection, channel, exchange and queue here
        xprint("ShoppingEventProducer initialize_rabbitmq() called")
        self.connection = pika.BlockingConnection(pika.ConnectionParameters("localhost"))
        self.channel = self.connection.channel()
        self.channel.exchange_declare(exchange="shopping_events_exchange",
                                      exchange_type="x-consistent-hash",
                                      durable=True)
        self.channel.queue_declare("shopping_events_dead_letter_queue")

    def publish(self, shopping_event):
        xprint("ShoppingEventProducer: Publishing shopping event {}".format(vars(shopping_event)))
        # To implement - publish a message to the Rabbitmq here
        # Use json.dumps(vars(shopping_event)) to convert the shopping_event object to JSON
        shopping_event_dict = vars(shopping_event)
        shopping_event_json = json.dumps(shopping_event_dict)
        product_number = shopping_event_dict.get("product_number")

        self.channel.basic_publish(exchange="shopping_events_exchange",
                                   routing_key=product_number,
                                   body=shopping_event_json)

    def close(self):
        # Do not edit this method
        self.channel.close()
        self.connection.close()
