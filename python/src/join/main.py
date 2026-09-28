import os
import logging
import signal
import bisect

from common import middleware, message_protocol, fruit_item

MOM_HOST = os.environ["MOM_HOST"]
INPUT_QUEUE = os.environ["INPUT_QUEUE"]
OUTPUT_QUEUE = os.environ["OUTPUT_QUEUE"]
SUM_AMOUNT = int(os.environ["SUM_AMOUNT"])
SUM_PREFIX = os.environ["SUM_PREFIX"]
AGGREGATION_AMOUNT = int(os.environ["AGGREGATION_AMOUNT"])
AGGREGATION_PREFIX = os.environ["AGGREGATION_PREFIX"]
TOP_SIZE = int(os.environ["TOP_SIZE"])


class JoinFilter:

    def __init__(self):
        self.input_queue = middleware.MessageMiddlewareQueueRabbitMQ(
            MOM_HOST, INPUT_QUEUE
        )
        self.output_queue = middleware.MessageMiddlewareQueueRabbitMQ(
            MOM_HOST, OUTPUT_QUEUE
        )
        self.state_by_client = {}
        signal.signal(signal.SIGTERM, self._handle_sigterm)

    def _handle_sigterm(self, signum, frame):
        logging.info("Received SIGTERM signal")
        self.input_queue.stop_consuming()

    def process_messsage(self, message, ack, nack):
        logging.info("Received partial top")
        client_id, _msg_type, payload = message_protocol.internal.deserialize(message)
        client_state = self.state_by_client.setdefault(
            client_id, {"candidates": [], "partials_received": 0}
        )
        # Sum already partitions by fruit, so no fruit can show up in more
        # than one aggregation instance's partial top: a plain insort is
        # enough here
        for fruit, amount in payload:
            bisect.insort(
                client_state["candidates"], fruit_item.FruitItem(fruit, amount)
            )
        client_state["partials_received"] += 1

        # Each aggregation instance sends exactly one partial top per client
        # (its own SUM_AMOUNT barrier), never a separate EOF here 
        if client_state["partials_received"] == AGGREGATION_AMOUNT:
            self._send_final_top(client_id)

        ack()

    def _send_final_top(self, client_id):
        logging.info("Merged partial tops from every aggregation instance")
        candidates = self.state_by_client.pop(client_id)["candidates"]
        top_chunk = list(candidates[-TOP_SIZE:])
        top_chunk.reverse()
        payload = [(item.fruit, item.amount) for item in top_chunk]
        self.output_queue.send(
            message_protocol.internal.serialize(
                client_id, message_protocol.internal.MsgType.DATA, payload
            )
        )

    def start(self):
        self.input_queue.start_consuming(self.process_messsage)
        self.input_queue.close()
        self.output_queue.close()


def main():
    logging.basicConfig(level=logging.INFO)
    join_filter = JoinFilter()
    join_filter.start()

    return 0


if __name__ == "__main__":
    main()
