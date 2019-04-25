#!/usr/bin/env python3
#
# mqtt-csv-writer
# adam@msys.se 2019
#
import os
import sys
import logging
import json
from datetime import datetime
import paho.mqtt.client as mqtt

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)-6s [%(module)s::%(funcName)s] %(message)s")
log = logging.getLogger(__name__)


class MqttCsvWriter():
    def __init__(self):
        log.info("msys mqtt-csv-writer")
        self.client = None
        self.mqtt_topic = os.getenv('MQTT_TOPIC', '/some/topic')
        self.csv_format = os.getenv('CSV_FORMAT', '{timestamp};{topic};{payload}\n')
        self.csv_header = os.getenv('CSV_HEADER', 'date;time;topic;payload\n')
        self.dt_format = os.getenv('DT_FORMAT', '%Y-%m-%d;%H:%M:%S')
        self.csv_delim = os.getenv('CSV_DELIM', ';')
        self.csv_file_pattern = os.getenv('CSV_OUTPUT', 'output-%Y%m%d.csv')
        self.csv_write_header = True
        self.fcsv = None

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
        log.info("connected to mqtt server")
        self.client.subscribe(self.mqtt_topic)
        
    def on_message(self, client, userdata, msg):
        log.debug('topic %r, payload %r', msg.topic, msg.payload)

        if str(msg.payload).endswith('/json'):
            self.csv_out_json(msg)
        else:
            self.csv_out_plain(msg)

        # set header done
        self.csv_write_header = False

    def csv_out_plain(self, msg):
        dt = datetime.now().strftime(self.dt_format)
        csv_file = datetime.now().strftime(self.csv_file_pattern)
        out = str(self.csv_format).format(timestamp=str(dt), topic=str(msg.topic), payload=float(msg.payload))
        log.debug(out)
        
        # check if file exists
        if os.path.exists(csv_file):
            # open if needed
            if self.fcsv is None:
                self.fcsv = open(csv_file, 'a')
        else:
            # close old file
            if self.fcsv: self.fcsv.close()

            # open new for writing
            self.fcsv = open(csv_file, 'w')

            # write header
            if self.csv_write_header:
                self.fcsv.write(self.csv_header)

        # write line
        self.fcsv.write(out)

    def csv_out_json(self, msg):
        j = json.loads(msg.payload)
        log.info(repr(j))

        # write header
        if self.csv_write_header:
            cols = []
            for key,value in j.items():
                cols.append(key)
            print(cols.join(';'))

        # write values
        cols = []
        for key,value in j.items():
            cols.append(value)

        print(cols.join(';'))


    def close(self):
        self.fcsv.close()

    def run(self):
        self.connect()
        


try:
    mr = MqttCsvWriter()
    mr.run()
except KeyboardInterrupt:
    mr.close()
except:
    sys.exit(0)




