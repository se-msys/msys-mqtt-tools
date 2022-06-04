#!/usr/bin/env python3
#
# mqtt-psql-writer
# mada@msys.se 2022
#
import os
import sys
import logging
import json
from re import match
from datetime import datetime
import paho.mqtt.client as mqtt
import postgresql

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)-6s [%(module)s::%(funcName)s] %(message)s")
log = logging.getLogger(__name__)


class MqttPsqlWriter():
    def __init__(self):
        log.info("msys mqtt-psql-writer")
        self.client = None
        self.mqtt_topic = os.getenv('MQTT_TOPIC', '/#')
        self.sqlt_topic = os.getenv('SQL_INSERT_TOPIC', "INSERT INTO topic (name) VALUES ($1)")
        self.sqlt_payload = os.getenv('SQL_INSERT_PAYLOAD', "INSERT INTO payload (topic_id,value,payload) VALUES ((SELECT id FROM topic WHERE name=$1), $2, $3)")
        self.pg_url = os.getenv('PG_URL', 'pq://postgres@localhost/mqtt')
        self.db = None

    def connect(self):
        # database
        try:
            self.db = postgresql.open(self.pg_url)
            log.info('connected to database')
            func = self.db.proc('version()')
            log.info(str(func()))
        except:
            log.error('unable to connect to database (%s)', self.pg_url)
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
        
    def on_message(self, client, userdata, msg):
        log.debug('topic %r, payload %r', msg.topic, msg.payload)
        self.insert(msg)

    def insert(self, msg):
        # prepare statements
        try:
            self.dbpsp = self.db.prepare(self.sqlt_payload)
            self.dbpst = self.db.prepare(self.sqlt_topic)
        except:
            log.error('sql statement error')

        # insert topic
        try:
            self.dbpst(str(msg.topic))
            log.info('inserted new topic %s', msg.topic)
        except:
            log.debug('topic %s already registered in database', msg.topic)

        # check if numeric or string
        try:
            value = float(msg.payload)
            payload = None
            log.debug('value is numeric')
        except ValueError:
            value = None
            payload = str(msg.payload)
            log.debug('value is not numeric')

        # insert payload/value
        try:
            self.dbpsp(str(msg.topic), value, payload)
            log.debug('successfully inserted values')
        except:
            log.error('unable to insert values')

    def close(self):
        self.db.close()

    def run(self):
        self.connect()
        


try:
    mr = MqttPsqlWriter()
    mr.run()
except KeyboardInterrupt:
    mr.close()
except:
    sys.exit(0)




