import time
import pika
from pika.exchange_type import ExchangeType
import queue
from collections import namedtuple
import logging
from functools import partial
import json
from threading import Thread
import sys
import os

varys_message = namedtuple("varys_message", "basic_deliver properties body")


class consumer(Thread):

    exchange_type = ExchangeType.topic

    def __init__(
        self,
        received_messages,
        configuration,
        log_file,
        log_level,
    ):

        Thread.__init__(self)

        self._messages = received_messages

        self._log = init_logger(configuration.profile, log_file, log_level)

        self._should_reconnect = configuration.reconnect
        self._reconnect_delay = 10
        self._connection = None
        self._channel = None
        self._closing = False
        self._consumer_tag = None
        self._consuming = False
        self._prefetch_count = configuration.prefetch_count

        self._exchange = configuration.exchange
        self._queue = configuration.queue

        self._routing_key = configuration.routing_key
        self._sleep_interval = configuration.sleep_interval

        self._parameters = pika.ConnectionParameters(
            host=configuration.ampq_url,
            port=configuration.port,
            credentials=pika.PlainCredentials(
                username=configuration.username, password=configuration.password
            ),
        )

    def __connect(self):
        self._connection = pika.SelectConnection(
            parameters=self._parameters,
            on_open_callback=self.__on_connection_open,
            on_open_error_callback=self.__on_connection_open_error,
            on_close_callback=self.__on_connection_closed,
        )
        self._connection.ioloop.start()

    def __on_connection_open(self, _unused_connection):
        self._log.info(f"Successfully connected to server")
        self.__open_channel()

    def __on_connection_open_error(self, _unused_connection, err):
        self._log.error(f"Failed to connect to server due to {err}")
        self.__reconnect()

    def __on_connection_closed(self, _unused_connection, reason):
        self._channel = None
        if self._closing:
            self._connection.ioloop.stop()
        else:
            self._log.warning(f"Connection closed, reconnect necessary: {reason}")
            self.__reconnect()

    def __reconnect(self):
        if self._should_reconnect:
            self.stop()
            time.sleep(self._reconnect_delay)
            self._log.warning(f"Reconnecting after {self._reconnect_delay} seconds")
            self.__connect()
        else:
            self._log.info(
                f"Reconnection was not set to re-connect after disconnection so closing"
            )

    def close_connection(self):
        self._consuming = False
        if self._connection.is_closing or self._connection.is_closed:
            self._log.info("Connection is closing or already closed")
        else:
            self._log.info("Closing connection")
            self._connection.close()

    def __open_channel(self):
        self._log.info("Creating a new channel")
        self._connection.channel(on_open_callback=self.__on_channel_open)

    def __on_channel_open(self, channel):
        self._channel = channel
        self._log.info("Channel opened")
        self.__add_on_channel_close_callback()
        self.__setup_exchange(self._exchange)

    def __add_on_channel_close_callback(self):
        self._log.info("Adding channel on closed callback")
        self._channel.add_on_close_callback(self.__on_channel_closed)

    def __on_channel_closed(self, channel, reason):
        self._log.warning(f"Channel {channel} was closed: {reason}")
        self.close_connection()

    def __setup_exchange(self, exchange_name):
        self._log.info(f"Declaring exchange - {exchange_name}")
        self._channel.exchange_declare(
            exchange=exchange_name,
            exchange_type=self.exchange_type,
            callback=self.__on_exchange_declareok,
            durable=True,
        )

    def __on_exchange_declareok(self, _unused_frame):
        self._log.info("Exchange successfully declared")
        self.__setup_queue(self._queue)

    def __setup_queue(self, queue_name):
        self._log.info(f"Declaring queue: {queue_name}")
        q_callback = partial(self.__on_queue_declareok, queue_name=queue_name)
        self._channel.queue_declare(
            queue=queue_name,
            callback=q_callback,
            durable=True,
        )

    def __on_queue_declareok(self, _unused_frame, queue_name):
        self._log.info(
            f"Binding queue {queue_name} to exchange: {self._exchange} with routing key {self._routing_key}"
        )
        self._channel.queue_bind(
            queue_name,
            self._exchange,
            routing_key=self._routing_key,
            callback=self.__on_bindok,
        )

    def __on_bindok(self, _unused_frame):
        self._log.info("Queue bound successfully")
        self.__set_qos()

    def __set_qos(self):
        self._channel.basic_qos(
            prefetch_count=self._prefetch_count, callback=self.__on_basic_qos_ok
        )

    def __on_basic_qos_ok(self, _unused_frame):
        self._log.info(f"QOS set to: {self._prefetch_count}")
        self.__start_consuming()

    def __start_consuming(self):
        self._log.info("Issuing consumer RPC commands")
        self.__add_on_cancel_callback()
        self._consumer_tag = self._channel.basic_consume(
            self._queue,
            self.__on_message,
        )
        self._consuming = True

    def __add_on_cancel_callback(self):
        self._log.info("Adding consumer cancellation callback")
        self._channel.add_on_cancel_callback(self.__on_consumer_cancelled)

    def __on_consumer_cancelled(self, method_frame):
        self._log.info(
            f"Consumer cancelled remotely, now shutting down: {method_frame}"
        )
        if self._channel:
            self._channel.close()

    def __on_message(self, _unused_channel, basic_deliver, properties, body):
        message = varys_message(basic_deliver, properties, body)
        self._log.info(
            f"Received Message: # {message.basic_deliver.delivery_tag} from {message.properties.app_id}, {message.body}"
        )
        self._messages.put(message)
        self.__acknowledge_message(message.basic_deliver.delivery_tag)

    def __acknowledge_message(self, delivery_tag):
        self._log.info(f"Acknowledging message: {delivery_tag}")
        self._channel.basic_ack(delivery_tag)

    def __stop_consuming(self):
        if self._channel:
            self._log.info(
                "Sending a Basic.Cancel command to central command (stopping message consumption)"
            )
            stop_consume_callback = partial(
                self.__on_cancelok, consumer_tag=self._consumer_tag
            )
            self._channel.basic_cancel(self._consumer_tag, stop_consume_callback)

    def __on_cancelok(self, _unused_frame, consumer_tag):
        self._consuming = False
        self._log.info(
            f"Broker acknowledged the cancellation of the consumer: {consumer_tag}"
        )
        self.__close_channel()

    def __close_channel(self):
        self._log.info("Closing the channel")
        self._channel.close()

    def run(self):
        try:
            self._connection = self.__connect()
            self._connection.ioloop.start()
        except KeyboardInterrupt:
            self.stop()

    def stop(self):
        if not self._closing:
            self._closing = True
            self._log.info("Stopping as instructed")
            if self._consuming:
                self.__stop_consuming()
                self._connection.ioloop.start()
            else:
                self._connection.ioloop.stop()
            self._log.info("Stopped as instructed")


class producer(Thread):
    def __init__(self, to_send, configuration, log_file, log_level):
        # username, password, queue, ampq_url, port, log_file, exchange="", routing_key="default", sleep_interval=5
        Thread.__init__(self)

        self._log = init_logger(configuration.profile, log_file, log_level)

        self._message_queue = to_send

        self._connection = None
        self._channel = None
        self._sleep_interval = configuration.sleep_interval

        self._exchange = configuration.exchange
        self._exchange_type = ExchangeType.topic
        self._queue = configuration.queue
        self._routing_key = configuration.routing_key

        self._deliveries = None
        self._acked = None
        self._nacked = None
        self._message_number = None

        self._stopping = False

        self._parameters = pika.ConnectionParameters(
            host=configuration.ampq_url,
            port=configuration.port,
            credentials=pika.PlainCredentials(
                username=configuration.username, password=configuration.password
            ),
        )

        self._message_properties = pika.BasicProperties(
            content_type="json", delivery_mode=2
        )

    def __connect(self):
        self._log.info("Connecting to broker")
        return pika.SelectConnection(
            self._parameters,
            on_open_callback=self.__on_connection_open,
            on_open_error_callback=self.__on_connection_open_error,
            on_close_callback=self.__on_connection_closed,
        )

    def __on_connection_open(self, _unused_connection):
        self._log.info("Connection to broker successfully opened")
        self.__open_channel()

    def __on_connection_open_error(self, _unused_connection, error):
        self._log.error(
            f"Connection attempt to broker failed, attempting to re-open in 10 seconds: {error}"
        )
        self._connection.ioloop.call_later(10, self._connection.ioloop.stop)

    def __on_connection_closed(self, _unused_connection, reason):
        self._channel = None
        if self._stopping:
            self._connection.ioloop.stop()
        else:
            self._log.warning(
                f"Connection to broker closed, will attempt to re-connect in 10 seconds: {reason}"
            )
            self._connection.ioloop.call_later(10, self._connection.ioloop.start)

    def __open_channel(self):
        self._log.info("Creating a new channel")
        self._connection.channel(on_open_callback=self.__on_channel_open)

    def __on_channel_open(self, channel):
        self._log.info("Channel successfully opened")
        self._channel = channel
        self.__add_on_channel_close_callback()
        self.__setup_exchange(self._exchange)

    def __add_on_channel_close_callback(self):
        self._log.info("Adding channel close callback")
        self._channel.add_on_close_callback(self.__on_channel_closed)

    def __on_channel_closed(self, channel, reason):
        self._log.warning(f"Channel {channel} was closed by broker: {reason}")
        self._channel = None
        if not self._stopping:
            self._connection.close()

    def __setup_exchange(self, exchange_name):
        self._log.info(f"Declaring exchange: {exchange_name}")
        self._channel.exchange_declare(
            exchange=exchange_name,
            exchange_type=self._exchange_type,
            callback=self.__on_declare_exchangeok,
            durable=True,
        )

    def __on_declare_exchangeok(self, _unused_frame):
        self._log.info("Exchange declared")
        self.__setup_queue(self._queue)

    def __setup_queue(self, queue):
        self._log.info(f"Declaring queue {queue}")
        self._channel.queue_declare(
            queue=queue, durable=True, callback=self.__on_queue_declareok
        )

    def __on_queue_declareok(self, _unused_frame):
        self._log.info(
            f"Binding {self._exchange} to {self._queue} with {self._routing_key}"
        )
        self._channel.queue_bind(
            self._queue,
            self._exchange,
            routing_key=self._routing_key,
            callback=self.__on_bindok,
        )

    def __on_bindok(self, _unused_frame):
        self._log.info("Queue successfully bound")
        self.__start_publishing()

    def __start_publishing(self):
        self._log.info(
            "Issuing consumer delivery confirmation commands and sending first message"
        )
        self.__enable_delivery_confirmations()
        self.__send_if_queued()

    def __enable_delivery_confirmations(self):
        self._log.info("Issuing Confirm.Select RPC command")
        self._channel.confirm_delivery(self.__on_delivery_confirmation)

    def __on_delivery_confirmation(self, method_frame):
        confirmation_type = method_frame.method.NAME.split(".")[1].lower()
        self._log.info(
            f"Received {confirmation_type} for delivery tag: {method_frame.method.delivery_tag}"
        )
        if confirmation_type == "ack":
            self._acked += 1
        elif confirmation_type == "nack":
            self._nacked += 1
        self._deliveries.remove(method_frame.method.delivery_tag)
        self._log.info(
            f"Published {self._message_number} messages, {len(self._deliveries)} have yet to be confirmed, "
            f"{self._acked} were acked, {self._nacked} were nacked"
        )

    def __send_if_queued(self):
        try:
            to_send = self._message_queue.get(block=False)
            self.publish_message(to_send)
        except queue.Empty:
            self._connection.ioloop.call_later(
                self._sleep_interval, self.__send_if_queued
            )

    def __close_channel(self):
        if self._channel is not None:
            self._log.info("Closing the channel")
            self._channel.close()

    def __close_connection(self):
        if self._connection is not None:
            self._log.info("Closing connection")
            self._channel.close()

    def publish_message(self, message):
        if self._channel is None or not self._channel.is_open:
            return False

        try:
            message_str = json.dumps(message, ensure_ascii=False)
        except TypeError:
            self._log.error(f"Unable to serialise message into json: {str(message)}")

        self._log.info(f"Sending message: {json.dumps(message)}")
        self._channel.basic_publish(
            self._exchange,
            self._routing_key,
            message_str,
            self._message_properties,
            mandatory=True,
        )

        self._message_number += 1
        self._deliveries.append(self._message_number)
        self._message_queue.task_done()
        self._log.info(f"Published message # {self._message_number}")

        self.__send_if_queued()

    def run(self):
        self._connection = None
        self._deliveries = []
        self._acked = 0
        self._nacked = 0
        self._message_number = 0

        try:
            self._connection = self.__connect()
            self._connection.ioloop.start()
        except KeyboardInterrupt:
            self.stop()
            if self._connection is not None and not self._connection.is_closed:
                # Finish closing
                self._connection.ioloop.start()

    def stop(self):
        self._log.info("Stopping publisher")
        self._stopping = True
        self.__close_channel()
        self.__close_connection()


def init_logger(name, log_path, log_level):
    log = logging.getLogger(name)
    log.propagate = False
    logging_fh = logging.FileHandler(log_path)
    log.setLevel(log_level)
    logging_fh.setFormatter(
        logging.Formatter("%(name)s\t::%(levelname)s::%(asctime)s::\t%(message)s")
    )
    log.addHandler(logging_fh)
    return log


class configurator:
    def __init__(self, profile, config_path=None):
        try:
            with open(config_path, "rt") as config_fh:
                config_obj = json.load(config_fh)
        except:
            print(
                "Configuration JSON does not appear to be valid or does not exist",
                file=sys.stderr,
            )
            sys.exit(11)

        if config_obj["version"] != "0.1":
            print(
                "Version number in the ROZ configuration file does not appear to be current, ensure configuration format is correct if errors are experienced",
                file=sys.stderr,
            )

        profile_dict = config_obj["profiles"].get(profile)
        if profile_dict:
            self.profile = profile
            self.username = str(profile_dict["username"])
            self.password = str(profile_dict["password"])
            self.ampq_url = str(profile_dict["ampq_url"])
            self.port = int(profile_dict["port"])
            self.queue = str(profile_dict["queue"])
            self.routing_key = str(profile_dict["routing_key"])
            self.exchange = str(profile_dict["exchange"])
            self.sleep_interval = int(profile_dict["sleep_interval"])
            self.prefetch_count = int(profile_dict["prefetch_count"])
            self.reconnect = bool(profile_dict["reconnect"])
        else:
            print(
                f"Varys configuration JSON does not appear to contain the specified profile {profile}",
                file=sys.stderr,
            )
            sys.exit(2)
