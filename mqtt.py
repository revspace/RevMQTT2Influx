import os
import time
import paho.mqtt.client as mqtt
from influxdb import InfluxDBClient

MQTT_HOST         = "test.mosquitto.org"
MQTT_PORT         = 1883
MQTT_SUBSCRIPTION = "revspace/sensors/#"

INFLUX_HOST = 'localhost'
INFLUX_PORT = 4444
INFLUX_DB   = 'mqtt'

# called when connected
def on_connect(client, userdata, flags, rc):
    print("Connected with result code "+str(rc))

    client.subscribe(MQTT_SUBSCRIPTION)

def send_update(topic, value):
    d = [ { "measurement": topic, "fields": { "value": value } } ]
    influx_client.write_points(d)

# called when MQTT message received
def on_message(client, userdata, msg):
    try:
        if msg.retain:
            # do not store retained messages, we don't know when they happened
            print("Ignoring retained message for topic '{:s}'".format(msg.topic))
        else:
            # store sample
            value = float(msg.payload.decode('utf-8').split(' ')[0])
            print("Got value {:f} for topic '{:s}'".format(value, msg.topic))

            send_update(msg.topic, value)
    except ValueError as e:
        print("Encountered error {} on topic {:s}".format(e, msg.topic))

# set up influxDB client
influx_client = InfluxDBClient(INFLUX_HOST, database=INFLUX_DB, use_udp=True, udp_port=INFLUX_PORT)

# connect to MQTT
mqtt_client = mqtt.Client()
mqtt_client.on_connect = on_connect
mqtt_client.on_message = on_message

mqtt_client.connect(MQTT_HOST, port=MQTT_PORT)
mqtt_client.loop_forever()
