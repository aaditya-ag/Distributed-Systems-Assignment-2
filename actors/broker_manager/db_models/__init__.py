# This folder contains definitions of DB tables.

"""
Database
- topic
- broker
- consumer
- producer
- topic_partition_broker mapping
- topic-partition-id_2_message
- broker to IP,PORT mappings
"""

"""
In memory

Metadata- 
- MasterBroker (dict)
    - dict(broker_id => brokers) # global_lock

    + add_brokers() 
        --> WAL update, acquire global_lock, update data structures, release lock, update DB

    + remove_brokers() 
        --> WAL update, acquire global_lock, update data structures, release lock, update DB

    + get_least_loaded_brokers()
        --> return [3 brokers]


- Topic 
    - dict( partition_id => broker_id ) # lock1
    - list( msg_index => [partition_id, broker_id]) # lock2
    - set (producer_ids), # lock3
    - dict (consumer_id => log_msg_offset) # lock4

    + add_producer()
        --> take lock3, update data structure, update DB  
    + remove_producer()
        --> take lock3, update data structure, update DB

    + add_consumer()
        --> take lock4, update data structure, update DB  
    + remove_consumer()
        --> take lock4, update data structure, update DB 

- MasterQueue :
    - dict(topic_name => Topic)  # global_lock

    + register_topic() 
        --> WAL update, make_new_topic_obj, Acquire lock, dict update, release lock, db update
        --> call master_broker.get_least_loaded_broker()

    + add_producer()
        --> WAL update
        --> calls topic.add_producer()

    + remove_producer()
        --> WAL update
        --> calls topic.remove_producer()
        
    + add_consumer()
        --> WAL update
        --> calls topic.add_consumer()

    + remove_consumer()
        --> WAL update
        --> calls topic.remove_consumer()

    + enqueue()
        --> WAL update
        --> 

    + dequeue()
        -- check size(), if not any msg, return
        --> WAL update
        --> 

    + size()
        --> Return size of remaining msgs to be read

Broker
    - Status bit
    - ip
    - port
    - num_partitions
    - lock
"""