import os
import logging
import bisect

from common import middleware, message_protocol, fruit_item

ID = int(os.environ["ID"])
MOM_HOST = os.environ["MOM_HOST"]
OUTPUT_QUEUE = os.environ["OUTPUT_QUEUE"]
SUM_AMOUNT = int(os.environ["SUM_AMOUNT"])
SUM_PREFIX = os.environ["SUM_PREFIX"]
AGGREGATION_AMOUNT = int(os.environ["AGGREGATION_AMOUNT"])
AGGREGATION_PREFIX = os.environ["AGGREGATION_PREFIX"]
TOP_SIZE = int(os.environ["TOP_SIZE"])


class AggregationFilter:

    def __init__(self):
        self.input_exchange = middleware.MessageMiddlewareExchangeRabbitMQ(
            MOM_HOST, AGGREGATION_PREFIX, [f"{AGGREGATION_PREFIX}_{ID}"]
        )
        self.output_queue = middleware.MessageMiddlewareQueueRabbitMQ(
            MOM_HOST, OUTPUT_QUEUE
        )
        self.fruit_top_by_client = {}

    def _process_data(self, client_id, fruit, amount):
        logging.info("Processing data message")
        fruit_top = self.fruit_top_by_client.setdefault(client_id, [])
        for i in range(len(fruit_top)):
            if fruit_top[i].fruit == fruit:
                fruit_top[i] = fruit_top[i] + fruit_item.FruitItem(fruit, amount)
                return
        bisect.insort(fruit_top, fruit_item.FruitItem(fruit, amount))

    def _process_eof(self, client_id):
        logging.info("Received EOF")
        fruit_top = self.fruit_top_by_client.pop(client_id, [])
        fruit_chunk = list(fruit_top[-TOP_SIZE:])
        fruit_chunk.reverse()
        payload = list(
            map(
                lambda fruit_item: (fruit_item.fruit, fruit_item.amount),
                fruit_chunk,
            )
        )
        self.output_queue.send(
            message_protocol.internal.serialize(
                client_id, message_protocol.internal.MsgType.DATA, payload
            )
        )

    def process_messsage(self, message, ack, nack):
        logging.info("Process message")
        client_id, msg_type, payload = message_protocol.internal.deserialize(message)
        if msg_type == message_protocol.internal.MsgType.DATA:
            fruit, amount = payload
            self._process_data(client_id, fruit, amount)
        else:
            self._process_eof(client_id)
        ack()

    def start(self):
        self.input_exchange.start_consuming(self.process_messsage)


def main():
    logging.basicConfig(level=logging.INFO)
    aggregation_filter = AggregationFilter()
    aggregation_filter.start()
    return 0


if __name__ == "__main__":
    main()
