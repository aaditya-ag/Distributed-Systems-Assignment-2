# How to setup ?

Run the following commands in the terminal.

- To setup database and initial config:
```
source setup.sh <YOUR_DATABASE_NAME_HERE>
```

- Run the application:
```
bash run_broker_manager.sh -p 5000
```


Run 3 Broker Managers
```
source setup.sh wo_mgr_db && bash run_broker_manager.sh -p 5001 -d false
source setup.sh ro_mgr1_db && bash run_broker_manager.sh -p 5002 -d false
source setup.sh ro_mgr2_db && bash run_broker_manager.sh -p 5003 -d false
```