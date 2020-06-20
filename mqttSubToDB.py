#!/usr/bin/env python3
import os 
import paho.mqtt.client as mqtt
from time import sleep
from datetime import datetime
from MessageProcessor import *

# seed the pseudorandom number generator
from random import seed
from random import randint
# seed random number generator
seed(1)

# Get current working directory
dir_path = os.path.dirname(os.path.realpath(__file__))

# Read MQTT configuration from json_file
with open(dir_path + '/' + 'mqtt_configuration.json') as json_file:
    mqtt_config = json.load(json_file)

# Read DB configuration from json_file
with open(dir_path + '/' + 'mongodb_configuration.json') as json_file:
    db_config = json.load(json_file)

def mqtt_on_log(client, userdata, level, rc):
    print("rc: "+str(rc))

def mqtt_on_connect(client, userdata, flags, rc):
    print("Connected with result code "+str(rc))
    for topic in mqtt_config['subscribe_topics']:
        client.subscribe(topic, qos=mqtt_config['qos'])

def mqtt_on_message(client, userdata, msg):
    # TODO: Get current timestamp and store with msg
    m = MessageStruct()
    m.data = msg
    m.timestamp = datetime.now()
    userdata.append(m)

def mqtt_on_disconnect(client, userdata, rc):
    if rc != 0:
        print("Unexpected MQTT disconnection. Attempting to reconnect.")
        try:
            client.reconnect()
        except Exception as err:
            print("Exception error: {0}".format(err))

def mqtt_init(data):
    client = mqtt.Client(mqtt_config['client_id']+"_"+str(randint(0, 24576)), clean_session=mqtt_config['clean_session'], userdata=data)
    client.username_pw_set(mqtt_config['username'], password=mqtt_config['password'])
    client.connect(mqtt_config['server'], mqtt_config['port'], mqtt_config['keep_alive'])
    client.on_connect = mqtt_on_connect
    client.on_disconnect = mqtt_on_disconnect
    client.on_message = mqtt_on_message
    client.on_log = mqtt_on_log

    return client

if __name__ == '__main__':
    msg_processor = MessageProcessor(db_config)
    client = mqtt_init(msg_processor.msg_queue)
    client.loop_start()

    try:
        while(True):
            if(msg_processor.get_msg_queue_size() > 0):
                msg_processor.process_msg()
            else:
                sleep(1)
    except KeyboardInterrupt:
        print("Keyboard Interrupt")
    except Exception as err:
        print("Exception error: {0}".format(err))
    
    msg_processor.disconnect_db()
    client.loop_stop()
    client.disconnect()
