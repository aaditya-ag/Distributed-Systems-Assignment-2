# How to setup ?

Run the following commands in the terminal.

- To setup database and initial config:
```
source setup.sh <YOUR_DATABASE_NAME_HERE>
```

- Run the application:
```
bash run_broker.sh -p 5000
```

Run 3 Brokers
```
source setup.sh broker1_db && bash run_broker.sh -p 8000 -d false
source setup.sh broker2_db && bash run_broker.sh -p 8001 -d false
source setup.sh broker3_db && bash run_broker.sh -p 8002 -d false
```