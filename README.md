# msys-mqtt-tools #
Some handy MQTT-tools


## mqtt-csv-writer ##
  - Writes CSV files with timestamped filename
  - CSV header and format configurable with environmental variables
  - Uses UTC ISO 8601-format as timestamp


Dependencies
  - <https://pypi.org/project/paho-mqtt/>

## mqtt-psql-writer ##
  - Writes to PostgreSQL database
  - Separate topic and payload tables
  - Configurable with environmental variables

Dependencies
  - <https://pypi.org/project/paho-mqtt/>
  - <https://py-postgresql.readthedocs.io/>, <https://github.com/python-postgres/fe>


## Settings ##
The following environmental variables can be used

    Name                      Default
    MQTT_HOST                 localhost
    MQTT_PORT                 1883
    MQTT_TIMEOUT              3
    CSV_FORMAT                {timestamp};{topic};{payload}\n
    CSV_HEADER                timestamp;topic;payload\n
    CSV_OUTPUT                output-%Y%m%d.csv
    SQL_INSERT_TOPIC          INSERT INTO topic (name) VALUES ($1)
    SQL_INSERT_PAYLOAD        INSERT INTO payload (topic,value,payload) VALUES ((SELECT id FROM topic WHERE name=$1), $2, $3)
    PG_URL                    pq://postgres@localhost/mqtt

