from consumer_client import ConsumerClient
from producer_client import ProducerClient
load_balancer_addr = "127.0.0.1"
load_balancer_port = "5000"


FAIL = " FAILURE"
PASS = " SUCCESS"

TOPIC = "T9"
MESSAGE = "Hello, World"

def run_test():
    producer = ProducerClient(loadBalancerAddr=load_balancer_addr, loadBalancerPort=load_balancer_port)
    producer.register(TOPIC)
    producer.send(TOPIC, MESSAGE)

    consumer = ConsumerClient(loadBalancerAddr=load_balancer_addr, loadBalancerPort=load_balancer_port)
    
    result = "REGISTER :: "
    status, message = consumer.register(TOPIC)
    result += FAIL if int(status) < 0 else PASS
    print(message)
    print(result)

    result = "SIZE :: "
    status,message = consumer.size(TOPIC)
    result += FAIL if int(status) < 0 or int(message) != 1 else PASS
    print(message)
    print(result)

    result = "HAS_NEXT :: " 
    status = consumer.has_next(TOPIC)
    result += FAIL if not status else PASS
    # print(message)
    print(result)

    result = "GET_NEXT :: "
    status, message = consumer.get_next(TOPIC)
    result += FAIL if int(status) < 0 or message != MESSAGE else PASS
    print(message)
    print(result)

    consumer.exit()
    producer.exit()

if __name__ == "__main__":
    run_test()