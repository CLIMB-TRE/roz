import datetime
import time
import paho.mqtt.client as mqtt
import os
import json
import sys
from copy import deepcopy

# TODO: write a proper logging class with verbosity / log priority concepts
# TODO: investigate encryption -> TLS is probably needed
def wrap_stderr(log_str):
    ts = int(time.time())
    datestamp = datetime.fromtimestamp(ts)
    print(f"[{datestamp}][ts: {ts}] " + str(log_str), file=sys.stderr)


class mqtt_client:
    def __init__(self, host, port, user, pwd, verbosity=int, persistence=False):
        self.host = host
        self.port = port
        self.user = user
        self.pwd = pwd
        self.verbosity = verbosity  # Vernosity 0 -> No logging, Verbosity 1 -> Only errors, Verbosity 2 -> EVERYTHING!!
        self.__messages = {}

        self.clean_session = not persistence

        self.client = mqtt.Client(clean_session=self.clean_session)

        def on_connect(client, userdata, flags, rc):
            if rc == 0:
                wrap_stderr(
                    f"Successfully connected to broker {self.host} on port {self.port} as user {self.user}"
                )
            else:
                if self.verbosity >= 1:
                    wrap_stderr(f"{mqtt.connack_string(rc)}")

        self.client.on_connect = on_connect

        self.client.username_pw_set(username=self.user, password=self.pwd)

        self.client.connect(self.host, self.port, keepalive=60)

    def subscribe(self, topic):
        def on_subscribe(client, userdata, mid, granted_qos):
            if self.verbosity == 2:
                wrap_stderr(f"Broker acknowledged subscription to topic: '{topic}'")
            self.__messages[topic] = {}

        def on_message(client, userdata, message):
            topic = message.topic
            payload = message.payload

            try:
                js_payload = json.loads(payload)
            except:
                if self.verbosity >= 1:
                    wrap_stderr(
                        f"Could not parse message payload as json, message payload received was:\n{payload}"
                    )

            self.__messages[topic].append(js_payload)

        self.client.on_message = on_message
        self.client.on_subscribe = on_subscribe

        self.client.subscribe(topic, qos=2)

        self.client.loop_forever()

    def consume_messages(self, topic):
        if len(self.messages) != 0:
            to_return = deepcopy(self.__messages[topic])
            self.__messages[topic] = []
            return set(to_return)
        else:
            return None

    def publish(self, topic, payload):
        def on_publish(client, userdata, mid):
            if self.verbosity == 2:
                wrap_stderr(f"Successfully published message with mid: {mid}")

        self.client.on_publish = on_publish

        try:
            pub_info = self.publish(topic=topic, payload=payload, qos=2, retain=False)
        except ValueError:
            if self.verbosity >= 1:
                wrap_stderr(
                    f"Failed to publish message: {pub_info.mid} because the {topic} in invalid or the payload is greater than 268435455 bytes"
                )

        if pub_info.rc != "MQTT_ERR_SUCCESS":
            if self.verbosity >= 1:
                wrap_stderr(
                    f"Failed to publish message: {pub_info.mid}. Error: {pub_info.rc}"
                )
