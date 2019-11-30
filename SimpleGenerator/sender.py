from azure.servicebus import ServiceBusService
import json
import csv
import random
import time

service_namespace = "<NAMESPACE_NAME>"
shared_access_key_name = "RootManageSharedAccessKey"
shared_access_key_value = "<KEY_VALUE>"

sbs = ServiceBusService(service_namespace,
                        shared_access_key_name=shared_access_key_name,
                        shared_access_key_value=shared_access_key_value)


def genMessage ():
    message = {
        "id": random.randint(1000,100000),
        "tt_1": random.randint(0, 100),
        "record": random.randint(0,3),
        "ts": time.time()

    }
    jsonMessage = json.dumps(message)
    return jsonMessage

for i in range(1, 10000):
	message = genMessage()
	sbs.send_event("<HUB_NAME>", message)
	print(message)

