#!/usr/bin/env python3
#
# mqtt-influx-writer
# mada@msys.se 2022
#
import os
import sys
import logging
import json
from datetime import datetime
import paho.mqtt.client as mqtt
from influxdb import InfluxDBClient

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)-6s [%(module)s::%(funcName)s] %(message)s")
log = logging.getLogger(__name__)


class MqttInfluxWriter():
    def __init__(self):
        log.info("msys mqtt-influx-writer")
        self.client = None
        self.mqtt_topic = os.getenv('MQTT_TOPIC', '/#')
        self.influx_host = os.getenv('INFLUX_HOST', '127.0.0.1')
        self.influx_db = os.getenv('INFLUX_DB', 'mqtt')
        self.db = None

    def connect(self):
        # database
        try:
            self.db = InfluxDBClient(host=self.influx_host, port=8086, database=self.influx_db)
            log.info('connected to database %s', self.influx_db)
        except:
            log.error('unable to connect to database')
            sys.exit(1)

        # mqtt
        self.client = mqtt.Client()
        self.client.on_connect = self.on_connect
        self.client.on_message = self.on_message
        self.client.connect(
            os.getenv('MQTT_HOST', 'localhost'),
            int(os.getenv('MQTT_PORT', 1883)),
            int(os.getenv('MQTT_TIMEOUT', 3)),
        )
        self.client.loop_forever()

    def on_connect(self, client, userdata, flags, rc):
        log.info("connected to mqtt server")
        self.client.subscribe(self.mqtt_topic)
        log.info("subscribed to %s", self.mqtt_topic)

    def on_message(self, client, userdata, msg):
        log.debug('topic %r, payload %r', msg.topic, msg.payload)
        data = {
            #'time':datetime.utcnow().strftime('%Y-%m-%dT%H:%M:%SZ'),
            'time':datetime.utcnow(),
            'measurement':'mqtt',
            'tags':{},
            'fields':{},
        }

        # check if numeric or string
        try:
            value = float(msg.payload)
            payload = None
            log.debug('value is numeric')
        except ValueError:
            value = None
            payload = str(msg.payload)
            log.debug('value is not numeric')

        # split topic
        pt = msg.topic.split('/')
        data['tags']['group'] = pt[1]
        data['tags']['node'] = pt[2]
        data['tags']['type'] = pt[3]

        # set values
        data['fields']['value'] = value

        # insert
        try:
            self.db.write_points([ data ])
            log.debug('inserted %r', data)
        except:
            log.error('unable to write to database')

    def close(self):
        self.db.close()

    def run(self):
        self.connect()


try:
    mr = MqttInfluxWriter()
    mr.run()
except KeyboardInterrupt:
    mr.close()
