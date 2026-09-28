import pika
import pika.exceptions

from .middleware import (
    MessageMiddlewareQueue,
    MessageMiddlewareExchange,
    MessageMiddlewareMessageError,
    MessageMiddlewareDisconnectedError,
    MessageMiddlewareCloseError,
)


def _connect(host):
    try:
        return pika.BlockingConnection(pika.ConnectionParameters(host=host))
    except pika.exceptions.AMQPConnectionError as e:
        raise MessageMiddlewareDisconnectedError(str(e)) from e


def _consume(channel, queue_name, on_message_callback):
    def _on_message(channel, method, properties, body):
        ack = lambda: channel.basic_ack(delivery_tag=method.delivery_tag)
        nack = lambda: channel.basic_nack(delivery_tag=method.delivery_tag, requeue=True)
        on_message_callback(body, ack, nack)

    try:
        channel.basic_consume(queue=queue_name, on_message_callback=_on_message)
        channel.start_consuming()
    except pika.exceptions.AMQPConnectionError as e:
        raise MessageMiddlewareDisconnectedError(str(e)) from e
    except pika.exceptions.AMQPError as e:
        raise MessageMiddlewareMessageError(str(e)) from e


def _stop_consuming(channel):
    # BlockingConnection isn't thread-safe, so this can't just call
    # channel.stop_consuming() directly if the caller isn't the thread
    # blocked in start_consuming() (e.g. a SIGTERM handler, which Python
    # always runs on the main thread). add_callback_threadsafe schedules the
    # actual stop on that connection's own thread, making this safe to call
    # from anywhere.
    try:
        channel.connection.add_callback_threadsafe(channel.stop_consuming)
    except pika.exceptions.AMQPConnectionError as e:
        raise MessageMiddlewareDisconnectedError(str(e)) from e


def _close(connection):
    try:
        connection.close()
    except pika.exceptions.AMQPConnectionError as e:
        raise MessageMiddlewareDisconnectedError(str(e)) from e
    except pika.exceptions.AMQPError as e:
        raise MessageMiddlewareCloseError(str(e)) from e

class MessageMiddlewareQueueRabbitMQ(MessageMiddlewareQueue):

    def __init__(self, host, queue_name):
        self.queue_name = queue_name
        self.connection = _connect(host)
        self.channel = self.connection.channel()
        self.channel.queue_declare(queue=queue_name, durable=True)
        # One unacked message per consumer at a time
        # ie: if sum_1 has unprocessed message and receives and EOF
        # message, it could react to the EOF message before processing 
        # the unprocessed old messages
        self.channel.basic_qos(prefetch_count=1)

    def start_consuming(self, on_message_callback):
        _consume(self.channel, self.queue_name, on_message_callback)

    def stop_consuming(self):
        _stop_consuming(self.channel)

    def send(self, message):
        try:
            self.channel.basic_publish(
                exchange="", routing_key=self.queue_name, body=message
            )
        except pika.exceptions.AMQPConnectionError as e:
            raise MessageMiddlewareDisconnectedError(str(e)) from e
        except pika.exceptions.AMQPError as e:
            raise MessageMiddlewareMessageError(str(e)) from e

    def close(self):
        _close(self.connection)


class MessageMiddlewareExchangeRabbitMQ(MessageMiddlewareExchange):

    def __init__(self, host, exchange_name, routing_keys):
        self.exchange_name = exchange_name
        self.routing_keys = routing_keys
        # An empty key list means "broadcast": every instance that binds to
        # this exchange gets its own copy, and the publisher doesn't need to
        # know how many consumers exist (needed for gateway -> sum EOF
        # fan-out, since the gateway doesn't know SUM_AMOUNT).
        exchange_type = "direct" if routing_keys else "fanout"

        self.connection = _connect(host)
        self.channel = self.connection.channel()
        self.channel.exchange_declare(
            exchange=exchange_name, exchange_type=exchange_type, durable=True
        )

        if exchange_type == "fanout":
            # Exclusive + broker-generated name: scoped to this connection,
            # deleted automatically when it closes. A publisher-only instance
            # (e.g. the gateway, which never calls start_consuming) ends up
            # with a queue nobody reads from, but it cleans itself up.
            result = self.channel.queue_declare(queue="", exclusive=True)
            self.queue_name = result.method.queue
            self.channel.queue_bind(exchange=exchange_name, queue=self.queue_name)
        else:
            # Deterministic name (not broker-generated) so a replica that
            # restarts rebinds to the *same* durable queue instead of losing
            # whatever was queued for it, and so a publisher can declare it
            # up front even if no consumer has started yet.
            self.queue_name = f"{exchange_name}." + ".".join(routing_keys)
            self.channel.queue_declare(queue=self.queue_name, durable=True)
            for routing_key in routing_keys:
                self.channel.queue_bind(
                    exchange=exchange_name,
                    queue=self.queue_name,
                    routing_key=routing_key,
                )

        self.channel.basic_qos(prefetch_count=1)

    def start_consuming(self, on_message_callback):
        _consume(self.channel, self.queue_name, on_message_callback)

    def stop_consuming(self):
        _stop_consuming(self.channel)

    def send(self, message):
        # direct: publish under the one key this instance represents (each
        # target gets its own instance, see sum/main.py's partitioning).
        # fanout: the key is ignored by RabbitMQ, every bound queue gets it.
        routing_key = self.routing_keys[0] if self.routing_keys else ""
        try:
            self.channel.basic_publish(
                exchange=self.exchange_name, routing_key=routing_key, body=message
            )
        except pika.exceptions.AMQPConnectionError as e:
            raise MessageMiddlewareDisconnectedError(str(e)) from e
        except pika.exceptions.AMQPError as e:
            raise MessageMiddlewareMessageError(str(e)) from e

    def close(self):
        _close(self.connection)
