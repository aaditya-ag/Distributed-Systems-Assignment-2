from producer_client import ProducerClient
import requests
load_balancer_addr = "127.0.0.1"
load_balancer_port = "5000"

FAIL = " FAILURE"
PASS = " SUCCESS"

TOPIC = "T2"

def run_test():
    producer = ProducerClient(loadBalancerAddr=load_balancer_addr, loadBalancerPort=load_balancer_port)

    result = "REGISTER :: "
    status, message = producer.register(TOPIC)
    result += FAIL if int(status) < 0 else PASS
    print(message)
    print(result)

    result = "ENQUEUE_DEFAULT_PARTITION :: "
    status, message = producer.send(TOPIC, "Hello World")
    result += FAIL if int(status) < 0 else PASS
    print(message)
    print(result)

    result = "ENQUEUE_INVALID_TOPIC :: "
    status, message = producer.send(TOPIC+"0", "Hello World")
    result += PASS if int(status) < 0 else FAIL
    print(message)
    print(result)

    print("DISPLAY_TOPIC_LOCATIONS :: ")
    producer.display_topic_locations(TOPIC)

    result = "ENQUEUE_PARTITION :: "
    partition_id = input("Enter a valid partition_id : ")
    status, message = producer.send(TOPIC, "Hello World", partition_id)
    result += FAIL if int(status) < 0 else PASS
    print(message)
    print(result)

    result = "ENQUEUE_PARTITION :: "
    partition_id = input("Enter an invalid partition_id : ")
    status, message = producer.send(TOPIC, "Hello World", partition_id)
    result += FAIL if int(status) < 0 else PASS
    print(message)
    print(result)

    producer.exit()

if __name__ == "__main__":
    resp = requests.post("http://127.0.0.1:5001/brokers", json={
        "broker_ip": "http://127.0.0.1",
        "broker_port": "8000"
    })

    resp = requests.post("http://127.0.0.1:5001/brokers", json={
        "broker_ip": "http://127.0.0.1",
        "broker_port": "8001"
    })

    resp = requests.post("http://127.0.0.1:5001/brokers", json={
        "broker_ip": "http://127.0.0.1",
        "broker_port": "8002"
    })
    run_test()