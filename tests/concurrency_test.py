import threading
import requests
import time

NUM_MESSAGES = 10
NUM_TOPICS = 3

def init():
    resp = requests.post("http://127.0.0.1:5001/brokers", json={
        "broker_ip": "http://127.0.0.1",
        "broker_port": "8000"
    })
    assert(resp.status_code == 201)
    
    resp = requests.post("http://127.0.0.1:5001/brokers", json={
        "broker_ip": "http://127.0.0.1",
        "broker_port": "8001"
    })
    assert(resp.status_code == 201)
    
    resp = requests.post("http://127.0.0.1:5001/brokers", json={
        "broker_ip": "http://127.0.0.1",
        "broker_port": "8002"
    })
    assert(resp.status_code == 201)

    for i in range(NUM_TOPICS):
        resp = requests.post("http://127.0.0.1:5000/topics", json={
            "topic_name": f"concurrent-test-T-{i}"
        })


def produce(topic_id):
    log_file = open(f"P{topic_id}", "w")
    topic_name = f"concurrent-test-T-{topic_id}"
    
    resp=requests.post("http://127.0.0.1:5000/producers", json={
        "topic_name": topic_name
    })
    assert(resp.status_code == 201)

    producer_id = resp.json()["producer_id"]
    for i in range(NUM_MESSAGES):
        msg = f"Msg-{i} for {topic_name}"
        resp = requests.post("http://127.0.0.1:5000/messages", json={
                "producer_id": producer_id,
                "topic_name": topic_name,
                "message": msg,
        })
        assert(resp.status_code == 201)
        log_file.write(msg)
        log_file.write("\n")
        log_file.flush()

    log_file.close()

def consume(topic_id):
    log_file = open(f"C{topic_id}", "w")
    topic_name = f"concurrent-test-T-{topic_id}"
    # time.sleep(5)
    resp=requests.post("http://127.0.0.1:5000/consumers", json={
        "topic_name": topic_name
    })
    assert(resp.status_code == 201)
    consumer_id = resp.json()["consumer_id"]

    counter = NUM_MESSAGES
    while(counter > 0):
        resp = requests.get("http://127.0.0.1:5000/messages", json={
                "consumer_id": consumer_id,
                "topic_name": topic_name,
        })
        time.sleep(0.1)
        if resp.status_code != 200:
            time.sleep(0.01)
            continue
        counter -= 1
        print(f"Topic-id = {topic_id}, counter = {counter}")
        msg = resp.json()["message"]
        log_file.write(msg)
        log_file.write("\n")
        log_file.flush()

    log_file.close()

def concurrent_run():
    threads = []
    for i in range(NUM_TOPICS):
        prod_thread = threading.Thread(target=produce, args=(i, ))
        prod_thread.start()
        threads.append(prod_thread)
    # for t in threads:
    #     t.join()
    # threads = []    
    for i in range(NUM_TOPICS):
        cons_thread = threading.Thread(target=consume, args=(i, ))
        cons_thread.start()
        threads.append(cons_thread)

    for t in threads:
        t.join()

def synchronous_run():
    pass

if __name__ == "__main__":
    init()
    start = time.time()
    concurrent_run()
    end = time.time()
    print(f"Elapsed Time = {1000*(end - start)} ms")