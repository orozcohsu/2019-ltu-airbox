#Orozco Hsu
#2019-07-02
#RS sensor data to kafka

import RPi.GPIO as GPIO
import dht11
import time
import datetime
import requests, json
from random import random

# initialize GPIO
GPIO.setwarnings(False)
GPIO.setmode(GPIO.BCM)
GPIO.cleanup()

# BCM:17
# BOARD: 11
instance = dht11.DHT11(pin=17)
#device id for join later
id="001"

#topic
temp="http://192.168.43.196:8082/topics/temperature"
humd="http://192.168.43.196:8082/topics/humidity"
headers = { "Content-Type" : "application/vnd.kafka.json.v2+json" }

while True:
    result = instance.read()
    if result.is_valid():
        #get sensor data
        temperature = "%0.2f" % float(result.temperature)
        humidity = "%d" % result.humidity
        print("t:",temperature)
        print("h:",humidity)
        #send data to kafka
        try:
            t=datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            rd=random()
            if temperature != "" and humidity != "":
                #rd for ksql join key
                payload = {"records":[{ "value": { "device_id":id,"timestamp":t,"Temperature":str(temperature),"rd":str(rd) }}]}
                r = requests.post(temp, data=json.dumps(payload), headers=headers)
               
                if r.status_code != 200:
                    print "Status Code(humd): " + str(r.status_code)
                    print r.text
                else:
                    print "temperature updated"
                
                payload = {"records":[{ "value": { "device_id":id,"timestamp":t,"Humidity":str(humidity),"rd":str(rd) }}]}
                r = requests.post(humd, data=json.dumps(payload), headers=headers)
                if r.status_code != 200:
                    print "Status Code(temp): " + str(r.status_code)
                    print r.text
                else:
                    print "humidity updated"

        except Exception as ex:
            print ex

    print "wait next 5 seconds"
    time.sleep(5)


