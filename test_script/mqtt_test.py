#!/usr/bin/env python3
import paho.mqtt.client as mqtt
from time import sleep

def mqtt_on_log(client, userdata, level, rc):
    print("rc: "+str(rc))

def mqtt_on_connect(client, userdata, flags, rc):
    print("Connected with result code "+str(rc))
    client.subscribe("topic/test")

def mqtt_on_message(client, userdata, msg):
    print(msg.topic)
    if msg.payload.decode() == "Hello world!":
        print("Yes!")

if __name__ == '__main__':
    client = mqtt.Client()
    client.connect("127.0.0.1",1883,60)

    client.on_connect = mqtt_on_connect
    client.on_message = mqtt_on_message
    client.loop_start()

    try:
        while(True):
            sleep(1)
    except KeyboardInterrupt:
        print("Keyboard Interrupt")
    
    client.loop_stop()
    client.disconnect()

    sleep(1)