# Test API endpoints for the broker manager

from fancy_print import *
import requests
import sys

base_url = "http://127.0.0.1"
port = 5000

if len(sys.argv) == 3:
    base_url = sys.argv[1]
    port = int(sys.argv[2])

url = base_url + ":" + str(port)

printInfo("Testing Broker Manager Endpoints")

#### Testing Heartbeats ####

# printInfo("1. Heartbeat check")

# try:
#     response: requests.Response = requests.get(url + "/")

#     assert response.status_code == 200
#     assert response.json()["status"] == "Success"

#     printSuccess("Test passed")
# except Exception as e:
#     printError("Test failed")

# print()

resp = requests.post("http://127.0.0.1:5001/brokers", json={
    "broker_ip": "http://127.0.0.1",
    "broker_port": "8000"
})
assert(resp.status_code == 201)
print("Broker 1 added")

resp = requests.post("http://127.0.0.1:5001/brokers", json={
    "broker_ip": "http://127.0.0.1",
    "broker_port": "8001"
})
assert(resp.status_code == 201)
print("Broker 2 added")

resp = requests.post("http://127.0.0.1:5001/brokers", json={
    "broker_ip": "http://127.0.0.1",
    "broker_port": "8002"
})
assert(resp.status_code == 201)
print("Broker 3 added")

#### Testing Topics API ####

printInfo("Test for Topics")

printInfo("1. Creating New Topic")

try:
    response: requests.Response = requests.post(
        url + "/topics", json={"topic_name": "T1"}
    )
    assert response.status_code == 201
    assert response.json()["status"] == "Success"

    printSuccess("Test passed")
except Exception as e:
    printError("Test failed")

printInfo("2. Creating Duplicate Topic")

try:
    response: requests.Response = requests.post(
        url + "/topics", json={"topic_name": "T1"}
    )

    assert response.status_code == 400
    assert response.json()["status"] == "Failure"

    printSuccess("Test passed")
except Exception as e:
    printError("Test failed")

print()

#### Testing Producers API ####

printInfo("Test for Producers")

printInfo("1. Creating New Producer")

try:
    response: requests.Response = requests.post(
        url + "/producers", json={"topic_name": "T1"}
    )
    assert response.status_code == 201
    assert response.json()["status"] == "Success"
    print(f'Producer id: {response.json()["producer_id"]}')
    print(f'Topic locations: {response.json()["topic_locations"]}')

    printSuccess("Test passed")
except Exception as e:
    printError("Test failed")

print()

#### Testing Topics API ####

printInfo("Test for Consumers")

printInfo("1. Creating New Consumers")

try:
    response: requests.Response = requests.post(
        url + "/consumers", json={"topic_name": "T1"}
    )
    assert response.status_code == 201
    assert response.json()["status"] == "Success"
    print(f'Consumer id: {response.json()["consumer_id"]}')

    response: requests.Response = requests.post(
        url + "/consumers", json={"topic_name": "T1"}
    )
    assert response.status_code == 201
    assert response.json()["status"] == "Success"

    printSuccess("Test passed")
except Exception as e:
    printError("Test failed")

printInfo("2. Creating Consumers on Non-existent topics")

try:
    response: requests.Response = requests.post(
        url + "/consumers", json={"topic_name": "T2"}
    )
    assert response.status_code == 400
    assert response.json()["status"] == "Failure"

    printSuccess("Test passed")
except Exception as e:
    printError("Test failed")

print()

#### Testing Enqueue API ####

printInfo("Test for Enqueue")

printInfo("1. Posting Messages")

try:
    response: requests.Response = requests.post(
        url + "/messages", json={"topic_name": "T1",
                                  "producer_id": 0,
                                  "message": "M1 for T1",
                                  "partition_id": 0
                                  }
    )
    assert response.status_code == 201

    response: requests.Response = requests.post(
        url + "/messages", json={"topic_name": "T1",
                                  "producer_id": 0,
                                  "message": "M2 for T1",
                                  "partition_id": 1
                                  }
    )
    assert response.status_code == 201

    response: requests.Response = requests.post(
        url + "/messages", json={"topic_name": "T1",
                                  "producer_id": 0,
                                  "message": "M3 for T1",
                                  "partition_id": 0
                                  }
    )
    assert response.status_code == 201

    response: requests.Response = requests.post(
        url + "/messages", json={"topic_name": "T1",
                                  "producer_id": 0,
                                  "message": "M4 for T1",
                                  "partition_id": 2
                                  }
    )
    assert response.json()["status"] == "Success"

    printSuccess("Test passed")
except Exception as e:
    printError("Test failed")

print()

#### Testing Dequeue API ####

printInfo("Test for Dequeue")

printInfo("1. Reading Messages")

try:
    for _ in range(4):
        response: requests.Response = requests.get(
            url + "/messages", json={"topic_name": "T1",
                                    "consumer_id": 0
                                    }
        )
        assert response.status_code == 200
        print(f'Message from queue: {response.json()["message"]}')

    printSuccess("Test passed")
except Exception as e:
    printError("Test failed")

print()

#### Testing Message Size API ####

printInfo("Test for Number of Unread Messages")

printInfo("1. Number of unread Messages")

try:
    response: requests.Response = requests.get(
        url + "/unread_messages", json={"topic_name": "T1",
                                "consumer_id": 1
                                }
    )
    assert response.status_code == 200
    print(f'Number of unread messages for this consumer: {response.json()["message_size"]}')

    printSuccess("Test passed")
except Exception as e:
    printError("Test failed")

print()


