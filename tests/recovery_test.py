import requests
import time

def test_recovery():

    topic_name = "recovery-test-T1"

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

    print("Creating topic")
    resp = requests.post("http://127.0.0.1:5000/topics", json={
        "topic_name": topic_name
    })

    print("Creating producer")
    resp = requests.post("http://127.0.0.1:5000/producers", json={
        "topic_name": topic_name
    })

    assert(resp.status_code == 201)
    producer_id = resp.json()["producer_id"]

    print("Creating consumer")
    resp = requests.post("http://127.0.0.1:5000/consumers", json={
        "topic_name": topic_name
    })
    assert(resp.status_code == 201)

    consumer_id = resp.json()["consumer_id"]

    print("Sending 10 messages")
    for i in range(10):
        resp = requests.post("http://127.0.0.1:5000/messages", json={
            "producer_id": producer_id,
            "topic_name": topic_name,
            "message": f"Msg-{i} for {topic_name}",
            "partition_id": i%3,
        })
        assert(resp.status_code == 201)
        print(f"Added Message-{i}")

    _ = input("Turn off broker 3 now to simulate failure recovery. Press Enter...")

    print("Reading 2 messages first")
    for i in range(2):
        resp = requests.get(f"http://127.0.0.1:5000/messages", json={
            "consumer_id": consumer_id,
            "topic_name": topic_name
        })
        assert(resp.status_code == 200)
        print(f"Read Message: {resp.json()['message']}")
    
    resp = requests.get(f"http://127.0.0.1:5000/messages", json={
        "consumer_id": consumer_id,
        "topic_name": topic_name
    })
    assert(resp.status_code == 400)
    print(f"Response received = {resp.status_code}")
    print("Broker-3 is down, hence message cant be retrived here")

    _ = input("Turn on broker 3 with debug false to test failure recovery. Press Enter...")
    print("The broker manager will update the status of broker 3 to UP")
    time.sleep(3)

    print("Reading 8 messages now")
    for i in range(8):
        resp = requests.get(f"http://127.0.0.1:5000/messages", json={
            "consumer_id": consumer_id,
            "topic_name": topic_name
        })
        assert(resp.status_code == 200)
        print(f"Read Message: {resp.json()['message']}")

if __name__ == '__main__':
    test_recovery()