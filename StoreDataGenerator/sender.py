import uuid
import time
import os
import random
import json
import pickle
import string
from kafka import KafkaProducer
from azure.servicebus import ServiceBusService

### Change to True if using Kafka for ingestion
kafka = False
STAY_ON = True

curDir = os.getcwd()
configFilePath = curDir[0:38] + "config.json"
with open(configFilePath, "r") as data:
    configJSON = json.load(data)
    ### EVENT HUB CONFIGURATION
    EVENT_HUB_NAMESPACE = configJSON["namespaceName"]
    SHARED_ACCESS_KEY_NAME = configJSON["keyName"]
    KEY_VALUE = configJSON["keyValue"]
    HUB_NAME = configJSON["hubName"]
    # KAFKA CONFIGURATION
    BOOTSTRAP_SERVER_A =  configJSON["worker-a-ip"] + ":9092"
    BOOTSTRAP_SERVER_B =  configJSON["worker-b-ip"] + ":9092"
    TOPIC_NAME = configJSON["topic"]


def random_generator(size=6, chars=string.ascii_uppercase + string.digits):
    return ''.join(random.choice(chars) for x in range(size))

### Change to True if using Kafka for ingestion
kafka = False
STAY_ON = True

curDir = os.getcwd()

if kafka == False:
    sbs = ServiceBusService(service_namespace=EVENT_HUB_NAMESPACE, shared_access_key_name=SHARED_ACCESS_KEY_NAME, shared_access_key_value=KEY_VALUE)
else:
    producer = KafkaProducer(bootstrap_servers=[BOOTSTRAP_SERVER_A, BOOTSTRAP_SERVER_B],value_serializer=lambda v: json.dumps(v).encode('utf-8'))

# Store ID list
storeids = list(range(1000, 1010))
print(len(storeids))

with open(curDir + "\\productJsons.pkl", "rb") as data:
    products = pickle.load(data)


while (STAY_ON):
    transactionTime = str(time.time()).split(".")[0]
    transactionID = str(storeids[random.randint(0,len(storeids)-1)])+"-"+ transactionTime + "-" + random_generator()
    storeID = transactionID.split("-")[0]
    numOfProducts = random.randint(1,20)
    cart = [products[random.randint(0, 298)] for i in range(0, numOfProducts)]
    
    reading = {
        "transactionID": transactionID,
        "storeID": storeID,
        "transactionTime": int(transactionTime),
        "cart": cart
    }

    if kafka == False:
        s = json.dumps(reading)
        # send to Azure Event Hub
        sbs.send_event(HUB_NAME, s)
        print(s)
    else:
        s = json.dumps(reading)
        # send to kafka
        producer.send(TOPIC_NAME, s)
        print(s)
