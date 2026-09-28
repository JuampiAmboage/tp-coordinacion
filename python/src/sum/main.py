import os
import logging
import signal
import threading
import hashlib

from common import middleware, message_protocol, fruit_item

ID = int(os.environ["ID"])
MOM_HOST = os.environ["MOM_HOST"]
INPUT_QUEUE = os.environ["INPUT_QUEUE"]
SUM_AMOUNT = int(os.environ["SUM_AMOUNT"])
SUM_PREFIX = os.environ["SUM_PREFIX"]
SUM_CONTROL_EXCHANGE = "SUM_CONTROL_EXCHANGE"
AGGREGATION_AMOUNT = int(os.environ["AGGREGATION_AMOUNT"])
AGGREGATION_PREFIX = os.environ["AGGREGATION_PREFIX"]


def _partition(fruit, amount_of_partitions):
    # Not Python's hash(): it's randomized per-process (PYTHONHASHSEED), so
    # two sum replicas could send the same fruit to different aggregation
    # instances. This has to agree across processes/containers.
    digest = hashlib.md5(fruit.encode("utf-8")).digest()
    return int.from_bytes(digest, "big") % amount_of_partitions


class SumFilter:
    def __init__(self):
        self.input_queue = middleware.MessageMiddlewareQueueRabbitMQ(
            MOM_HOST, INPUT_QUEUE
        )
        # Two separate connections, not one shared between threads: pika's
        # BlockingConnection isn't thread-safe, and this exchange is both
        # consumed (control_thread) and published to, as a relay, from the
        # data thread (see process_data_messsage).
        self.control_exchange = middleware.MessageMiddlewareExchangeRabbitMQ(
            MOM_HOST, SUM_CONTROL_EXCHANGE, []
        )
        self.control_exchange_relay = middleware.MessageMiddlewareExchangeRabbitMQ(
            MOM_HOST, SUM_CONTROL_EXCHANGE, []
        )
        self.data_output_exchanges = []
        for i in range(AGGREGATION_AMOUNT):
            data_output_exchange = middleware.MessageMiddlewareExchangeRabbitMQ(
                MOM_HOST, AGGREGATION_PREFIX, [f"{AGGREGATION_PREFIX}_{i}"]
            )
            self.data_output_exchanges.append(data_output_exchange)
        self.amount_by_fruit_by_client = {}
        # amount_by_fruit_by_client is written from the data-queue thread and
        # read/popped from the control-exchange thread, see start().
        self.lock = threading.Lock()
        signal.signal(signal.SIGTERM, self._handle_sigterm)

    def _handle_sigterm(self, signum, frame):
        logging.info("Received SIGTERM signal")
        self.input_queue.stop_consuming()
        self.control_exchange.stop_consuming()

    def _process_data(self, client_id, fruit, amount):
        logging.info(f"Process data")
        with self.lock:
            amount_by_fruit = self.amount_by_fruit_by_client.setdefault(client_id, {})
            amount_by_fruit[fruit] = amount_by_fruit.get(
                fruit, fruit_item.FruitItem(fruit, 0)
            ) + fruit_item.FruitItem(fruit, int(amount))

    def _process_eof(self, client_id):
        logging.info(f"Partitioning data messages")
        with self.lock:
            amount_by_fruit = self.amount_by_fruit_by_client.pop(client_id, {})

        for final_fruit_item in amount_by_fruit.values():
            target = _partition(final_fruit_item.fruit, AGGREGATION_AMOUNT)
            self.data_output_exchanges[target].send(
                message_protocol.internal.serialize(
                    client_id,
                    message_protocol.internal.MsgType.DATA,
                    [final_fruit_item.fruit, final_fruit_item.amount],
                )
            )

        logging.info(f"Broadcasting EOF message")
        for data_output_exchange in self.data_output_exchanges:
            data_output_exchange.send(
                message_protocol.internal.serialize(
                    client_id, message_protocol.internal.MsgType.EOF, []
                )
            )

    def process_data_messsage(self, message, ack, nack):
        client_id, msg_type, payload = message_protocol.internal.deserialize(message)
        if msg_type == message_protocol.internal.MsgType.DATA:
            fruit, amount = payload
            self._process_data(client_id, fruit, amount)
        else:
            # Whichever replica dequeues this (only one will, INPUT_QUEUE is
            # shared/competing) relays it on the control exchange instead of
            # flushing directly: dequeuing it here means every one of this
            # client's records that was published before it is *at least*
            # already dispatched to some replica (same-queue FIFO), so the
            # relay is what makes every replica's own flush safe to trust.
            self.control_exchange_relay.send(
                message_protocol.internal.serialize(
                    client_id, message_protocol.internal.MsgType.EOF, []
                )
            )
        ack()

    def process_control_message(self, message, ack, nack):
        client_id, _msg_type, _payload = message_protocol.internal.deserialize(message)
        self._process_eof(client_id)
        ack()

    def start(self):
        data_thread = threading.Thread(
            target=self.input_queue.start_consuming, args=(self.process_data_messsage,)
        )
        control_thread = threading.Thread(
            target=self.control_exchange.start_consuming,
            args=(self.process_control_message,),
        )
        data_thread.start()
        control_thread.start()
        data_thread.join()
        control_thread.join()

        self.input_queue.close()
        self.control_exchange.close()
        self.control_exchange_relay.close()
        for data_output_exchange in self.data_output_exchanges:
            data_output_exchange.close()

def main():
    logging.basicConfig(level=logging.INFO)
    sum_filter = SumFilter()
    sum_filter.start()
    return 0


if __name__ == "__main__":
    main()
