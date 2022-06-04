#!/usr/bin/env python3
#
# mqtt-csv-writer
# mada@msys.se 2022
#
import os
import sys
import logging
from datetime import datetime
import paho.mqtt.client as mqtt


# setup logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)-6s [%(module)s::%(funcName)s] %(message)s")
log = logging.getLogger(__name__)


"""
    MQTT CSV Writer
"""
class MqttCsvWriter():
    def __init__(self):
        log.info("msys mqtt-csv-writer")
        self.client = None
        self.mqtt_topic = os.getenv('MQTT_TOPIC', '/#')
        self.csv_format = os.getenv('CSV_FORMAT', '{timestamp};{topic};{payload}\n')
        self.csv_header = os.getenv('CSV_HEADER', 'timestamp;topic;payload\n')
        self.csv_file_pattern = os.getenv('CSV_OUTPUT', 'output-%Y%m%d.csv')

        log.info('Current settings:')
        log.info('  topic      %s', self.mqtt_topic)
        log.info('  header     %r', self.csv_header)
        log.info('  format     %r', self.csv_format)
        log.info('  output     %s', self.csv_file_pattern)


    def csv_out(self, msg):
        dt = datetime.utcnow()
        csv_file = dt.strftime(self.csv_file_pattern)
        out = self.csv_format.format(timestamp=dt.isoformat(), topic=msg.topic, payload=msg.payload.decode('utf-8'))
        
        # check if file exists
        if os.path.exists(csv_file):
            log.debug('appending to existing file "%s"', csv_file)

            with open(csv_file, 'a') as f:
                f.write(out)

        # else create new
        else:
            log.info('creating new file "%s"', csv_file)

            with open(csv_file, 'w') as f:
                f.write(self.csv_header)
                f.write(out)


    def connect(self):
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
        log.info("connected to mqtt broker")
        self.client.subscribe(self.mqtt_topic)
        

    def on_message(self, client, userdata, msg):
        log.debug('topic %r, payload %r', msg.topic, msg.payload)
        self.csv_out(msg)


    def close(self):
        pass


    def run(self):
        self.connect()
        


try:
    mr = MqttCsvWriter()
    mr.run()
except KeyboardInterrupt:
    mr.close()
except:
    sys.exit(0)




