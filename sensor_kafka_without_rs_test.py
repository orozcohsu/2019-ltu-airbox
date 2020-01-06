#Orozco Hsu
#2020-01-06

import time
import datetime
import requests, json
from random import random

#topic
temp="http://localhost:8082/topics/temperature"
humd="http://localhost:8082/topics/humidity"
headers = { "Content-Type" : "application/vnd.kafka.json.v2+json" }

while True:
    if 0==0:
        id = 888
        humidity = 55
        temperature = 39

        #send data to kafka
        try:
            t=datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            rd=random()
            if temperature != "" and humidity != "":
                #rd for ksql join key
                payload = {"records":[{ "value": { "device_id":id,"timestamp":t,"Temperature":str(temperature),"rd":str(rd) }}]}
                print("payload:",payload)
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


