import time
import pika
from pika.exchange_type import ExchangeType
from collections import namedtuple
import logging
from functools import partial
import json
import os

class varys_consumer:
    
    varys_message = namedtuple("varys_message", "basic_deliver properties body")
    
    exchange_type = ExchangeType.topic
    
    def __init__(self, username, password, queue, re_connect, ampq_url, port, log_file, exchange=None, routing_key="default", prefetch_count=1, blocking=True, durable=False):
        self._messages = []
        
        self._log = logging.getLogger("varys_consumer")
        logging.basicConfig(level=logging.DEBUG, format='%(levelname)s:%(asctime)s:%(message)s', filename=log_file)
        
        self._should_reconnect = re_connect
        self._reconnect_delay = 10
        self._connection = None
        self._channel = None
        self._closing = False
        self._consumer_tag = None
        self._consuming = False
        self._durable = durable
        self._prefetch_count = prefetch_count
        self._exchange = exchange
        self._queue = queue
        self._routing_key = routing_key
        self._blocking = blocking
                
        self._parameters = pika.ConnectionParameters(host=ampq_url, port=port, credentials=pika.PlainCredentials(username=username, password=password))
    
    def __connect(self):
        self._connection = pika.SelectConnection(parameters=self._parameters,
                                                 on_open_callback=self.__on_connection_open,
                                                 on_open_error_callback=self.__on_connection_open_error,
                                                 on_close_callback=self.__on_connection_closed)
        self._connection.ioloop.start()
        
    def __on_connection_open(self):
        self._log.info(f"Successfully connected to server")
        self.__open_channel()
        
    def __on_connection_open_error(self, _unused_connection, err):
        print(err)
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
            self._log.info(f"Reconnection was not set to re-connect after disconnection so closing")
        
    def close_connection(self):
        self._consuming = False
        if self._connection.is_closing or self._connection.is_closed:
            self._log.info('Connection is closing or already closed')
        else:
            self._log.info('Closing connection')
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
        self._log(f"Declaring exchange - {exchange_name}")
        self._channel.exchange_declare(exchange=exchange_name, exchange_type=self.exchange_type, callback=self.__on_exchange_declareok)
    
    def __on_exchange_declareok(self, _unused_frame):
        self._log.info("Exchange successfully declared")
        self.__setup_queue(self._queue)
            
    def __setup_queue(self, queue_name):
        self._log.info(f"Declaring queue: {queue_name}")
        q_callback = partial(self.__on_queue_declareok, queue_name=queue_name)
        self._channel.queue_declare(queue=queue_name, callback=q_callback, durable=self._durable)
    
    def __on_queue_declareok(self, _unused_frame, queue_name):
        self._log.info(f"Binding queue {queue_name} to exchange: {self._exchange} with routing key {self._routing_key}")
        self._channel.queue_bind(queue_name, self._exchange, routing_key=self._routing_key, callback=self.__on_bindok)
    
    def __on_bindok(self, _unused_frame, userdata):
        self._log.info("Queue bound successfully")
        self.__set_qos()
    
    def __set_qos(self):
        self._channel.basic_qos(prefetch_count=self._prefetch_count, callback=self.__on_basic_qos_ok)
    
    def __on_basic_qos_ok(self, _unused_frame):
        self._log.info(f"QOS set to: {self._prefetch_count}")
        self.__start_consuming()
    
    def __start_consuming(self):
        self._log.info("Issuing consumer RPC commands")
        self.__add_on_cancel_callback()
        self._consumer_tag = self._channel.basic_consume(self._queue, self.__on_message)
        self._consuming = True
    
    def __add_on_cancel_callback(self):
        self._log.info("Adding consumer cancellation callback")
        self._channel.add_on_cancel_callback(self.__on_consumer_cancelled)
    
    def __on_consumer_cancelled(self, method_frame):
        self._log.info(f"Consumer cancelled remotely, now shutting down: {method_frame}")
        if self._channel:
            self._channel.close()
    
    def __on_message(self, _unused_channel, basic_deliver, properties, body):
        message = self.varys_message(basic_deliver, properties, body)
        self._log.info(f"Received Message: # {message.basic_deliver.delivery_tag} from {message.properties.app_id}, {message.body}")
        self._messages.append(message)
        if not self._blocking:
            self.__acknowledge_message(basic_deliver.delivery_tag)
    
    def __acknowledge_message(self, delivery_tag):
        self._log.info(f"Acknowledging message: {delivery_tag}")
        self._channel.basic_ack(delivery_tag)
        
    def __stop_consuming(self):
        if self._channel:
            self._log.info("Sending a Basic.Cancel command to central command (stopping message consumption)")
            stop_consume_callback = partial(self.__on_cancelok, consumer_tag=self._consumer_tag)
            self._channel.basic_cancel(self._consumer_tag, stop_consume_callback)
    
    def __on_cancelok(self, _unused_frame, consumer_tag):
        self._consuming = False
        self._log.info(f"Broker acknowledged the cancellation of the consumer: {consumer_tag}")
        self.__close_channel()
    
    def __close_channel(self):
        self._log.info("Closing the channel")
        self._channel.close()

    def check_for_messages(self):
        if self._messages:
            return True
        else:
            return False

    def message_generator(self):
        if self._messages:
            self._log.info(f"Consuming {len(self._messages)} messages")
            for message in self._messages:
                if self._blocking:
                    self.__acknowledge_message(message.basic_deliver.delivery_tag)
                yield message
            self._messages = []
        else:
            return None    
    
    def run(self):
        while True:
            try:
                self._connection = self.__connect()
                self._connection.ioloop.start()
            except KeyboardInterrupt:
                self.stop()
                break
            self.__reconnect()
        
    
    def stop(self):
        if not self._closing:
            self._closing = True
            self._log.info('Stopping as instructed')
            if self._consuming:
                self.__stop_consuming()
                self._connection.ioloop.start()
            else:
                self._connection.ioloop.stop()
            self._log.info('Stopped as instructed')        
    
class varys_producer:
        
    def __init__(self, log_name=str, log_file=str, username=str, password=str, ampq_url=str, port=int, exchange=str, queue=str, routing_key="default", durable=False):
        self._log = logging.getLogger(log_name)
        logging.basicConfig(level=logging.DEBUG, format='%(levelname)s:%(asctime)s:%(message)s', filename=log_file)
        
        self._connection = None
        self._channel = None
        self._publish_interval = 1
        
        self._exchange = exchange
        self._exchange_type = ExchangeType.topic
        self._queue = queue
        self._routing_key = routing_key
        self._durable = durable
        
        self._deliveries = None
        self._acked = None
        self._nacked = None
        self._message_number = None
        
        self._stopping = False
        
        log_format = ('%(levelname) -10s %(asctime)s %(name) -30s %(funcName) -35s %(lineno) -5d: %(message)s')
        
        self._parameters = pika.ConnectionParameters(host=ampq_url, port=port, credentials=pika.PlainCredentials(username=username, password=password))
        
        self._message_properties = pika.BasicProperties(content_type="application/json", delivery_mode=pika.DeliveryMode.Persistent)
        
    def __connect(self):
        self._log.info("Connecting to broker")
        return pika.SelectConnection(self._parameters, 
                                     on_open_callback=self.__on_connection_open, 
                                     on_open_error_callback=self.__on_connection_open_error, 
                                     on_close_callback=self.__on_connection_closed)
    
    def __on_connection_open(self, _unused_connection):
        self._log.info("Connection to broker successfully opened")
        self.__open_channel()
    
    def __on_connection_open_error(self, _unused_connection, error):
        self._log.error(f"Connection attempt to broker failed, attempting to re-open in 10 seconds: {error}")
        self._connection.ioloop.call_later(10, self._connection.ioloop.stop)
        
    def __on_connection_closed(self, _unused_connection, reason):
        self._channel = None
        if self._stopping:
            self._connection.ioloop.stop()
        else:
            self._log.warning(f"Connection to broker closed, will attempt to re-connect in 10 seconds: {reason}")
            self._connection.ioloop.call_later(10, self._connection.ioloop.stop)
        
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
        self._log.info("Declaring exchange: {exchange_name}")
        self._channel.exchange_declare(exchange=exchange_name, exchange_type=self._exchange_type, callback=self.__on_declare_exchangeok)
    
    def __on_declare_exchangeok(self):
        self._log.info("Exchange declared")
        self.__setup_queue(self._queue)
    
    def __setup_queue(self, queue):
        self._log.info(f"Declaring queue {queue}")
        self._channel.queue_declare(queue=queue, durable=self._durable, callback=self.__on_queue_declareok)
    
    def __on_queue_declareok(self, _unused_frame):
        self._log.info(f"Binding {self._exchange} to {self._queue} with {self._routing_key}")
        self._channel.queue_bind(self._queue, self._exchange, routing_key=self._routing_key, callback=self.__on_bindok)
        
    def __on_bindok(self, _unused_frame):
        self._log.info("Queue successfully bound")
        self.__start_publishing()
    
    def __start_publishing(self):
        self._log.info("Issuing consumer delivery confirmation commands and sending first message")
        self.__enable_delivery_confirmations()
        self.__schedule_next_message()
    
    def __enable_delivery_confirmations(self):
        self._log.info("Issuing Confirm.Select RPC command")
        self._channel.confirm_delivery(self.__on_delivery_confirmation)
    
    def __on_delivery_confirmation(self, method_frame):
        confirmation_type = method_frame.method.NAME.split('.')[1].lower()
        self._log.info(f"Received {confirmation_type} for delivery tag: {method_frame.method.delivery_tag}")
        if confirmation_type == 'ack':
            self._acked += 1
        elif confirmation_type == 'nack':
            self._nacked += 1
        self._deliveries.remove(method_frame.method.delivery_tag)
        self._log.info(f"Published {self._message_number} messages, {len(self._deliveries)} have yet to be confirmed"
                       f"{self._acked} were acked, {self._nacked} were acked")

    def __schedule_next_message(self):
        self._log.info(f"Scheduling next message to publish in {self._publish_inverval} seconds")
        self._connection.ioloop.call_later(self._publish_interval, self.publish_message)
    
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
            return False

        self._channel.basic_publish(self._exchange, self._routing_key, message_str, self._message_properties)
        self._message_number += 1
        self._deliveries.append(self._message_number)
        self._log.info(f"Published message # {self._message_number}")
        
        self.__schedule_next_message()
        return True
        
    def run(self):
        while not self._stopping:
            self._connection = None
            self._deliveries = []
            self._acked = 0
            self._nacked = 0
            self._message_number = 0

            try:
                self._connection = self.connect()
                self._connection.ioloop.start()
            except KeyboardInterrupt:
                self.stop()
                if (self._connection is not None and
                        not self._connection.is_closed):
                    # Finish closing
                    self._connection.ioloop.start()

        self._log.info('Stopped')

    def stop(self):
        self._log.info("Stopping publisher")
        self._stopping = True
        self.__close_channel()
        self.__close_connection()
        
class configurator:
    if os.getenv("VARYS_CONFIG"):
        config_path = os.getenv("VARYS_CONFIG")

    def __init__(self, profile, config_path=None):
        with open(self.config_path, "rt") as config_fh:
            config_obj = json.load(config_fh)
        
        if config_obj.get(profile):
            self.username = config_obj["username"]
            self.password = config_obj["password"]
            self.host = config_obj["host"]
            self.port = config_obj["port"]
            self.queue = config_obj["queue"]