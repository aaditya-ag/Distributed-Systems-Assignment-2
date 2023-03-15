from fancy_print import *
import requests
import sys

base_url = "http://127.0.0.1"
port = 8000

if len(sys.argv) == 3:   
    base_url = sys.argv[1]
    port = int(sys.argv[2])

url = base_url + ":" + str(port) + "/"

printInfo("Running Broker Healthcheck")

try:
    response: requests.Response = requests.get(url)
    assert(response.status_code == 200)
    assert(response.json()["status"] == "Success")

    printSuccess("Test passed")
except Exception as e:
    printError("Test failed")




