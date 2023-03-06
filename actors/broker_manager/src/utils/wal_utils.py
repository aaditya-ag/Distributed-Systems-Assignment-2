from src import db
from db_models import (
    WALModel,
    TopicModel,
    ProducerModel,
    ConsumerModel,
    TPLMapModel,
    TPBMapModel,
    BrokerModel,
)

STATUS_BEGIN=0
STATUS_MEM=1
STATUS_SYNC=2
STATUS_DB=3
STATUS_ERR=-1
STATUS_END=STATUS_DB

sep=','#"__$#$__"

#OPERATION
INSERT=0
UPDATE=1
DELETE=2

class WAL_Entry():
    def __init__(self, table:str, operation:int, numargs:int, args:str):
        self.table = table
        self.operation = operation
        self.num_args = numargs
        self.args = args

    def decode_args(self):
        return decode_arg_string(self.num_args, self.args)

def generate_arg_string(num_args:int, args:list):
    result_str = ""
    for i in range(num_args):
        result_str += str(args[i])
        if(i == num_args-1): continue
        result_str += sep
    return result_str

def decode_arg_string(num_args:int, argstring:str):
    result = argstring.split(sep=sep)
    if not (len(result) == num_args): 
        print("FATAL: illegal WAL entry maybe found.")
        return None
    return result

# def generate_wal_entry(tablename, operation, num_args, args, stage):
#     return WALModel(
#         operation=operation,
#         table=tablename,
#         num_args=num_args,
#         args=generate_arg_string(num_args, args),
#         stage=stage
#     )

def log(tablename, operation, num_args, args, stage, begin_id=-1):
    log_entry=WALModel(
        begin_id=begin_id,
        operation=operation,
        table=tablename,
        num_args=num_args,
        args=generate_arg_string(num_args, args),
        stage=stage,   
    )

    db.session.add(log_entry)
    db.session.commit()

    return log_entry.id

def commit(walentry:WALModel):
    """
        tablename = [Topic, Producer, Consumer, Broker, TPLMap, TBPMap]
        operation = [INSERT, UPDATE]
        num_args #
        args #
    """
    table = walentry.table
    operation = walentry.operation
    args = walentry.decode_args()
    if operation == INSERT: 
        # switch(table)
        if table == "Topic":
            # check if topic not already present; add topic;
            if TopicModel.query.filter_by(name=args[0]).count() == 0:
                topic = TopicModel(name=args[0])
                db.session.add(topic)
                db.session.commit()
        elif table == "Producer":
            # check if given producer_id already present; add producer;
            if ProducerModel.query.filter_by(producer_id=int(args[0]), 
                                             topic=args[1]).count() == 0:
                prod = ProducerModel(producer_id=int(args[0]),
                                     topic=args[1])
                db.session.add(prod)
                db.session.commit()
        elif table == "Consumer":
            # check if consumer_id already present; add consumer;
            if ConsumerModel.query.filter_by(consumer_id=int(args[0]), 
                                             topic=args[1]).count() == 0:
                cons = ConsumerModel(consumer_id=int(args[0]),
                                     topic=args[1])
                db.session.add(cons)
                db.session.commit()
        elif table == "Broker":
            # check if broker not already present; add broker;
            if BrokerModel.query.filter_by(id=int(args[0]),
                                           ip=args[1],
                                           port=args[2]).count() == 0:
                broker = BrokerModel(id=int(args[0]),
                                     ip=args[1],
                                     port=args[2])
                db.session.add(broker)
                db.session.commit()
        elif table == "TPLMap":
            # check if entry not already present; add entry;
            if TPLMapModel.query.filter_by(topic_name=args[0],
                                           producer_id=int(args[1]),
                                           partition_id=int(args[2]),
                                           log_index=int(args[3])).count() == 0:
                tplentry = TPLMapModel(topic_name=args[0],
                                       producer_id=int(args[1]),
                                       partition_id=int(args[2]),
                                       log_index=int(args[3]))
                db.session.add(tplentry)
                db.session.commit()
        elif table == "TBPMap":
            # check if entry not already present; add entry;
            if TPBMapModel.query.filter_by(topic_name=args[0],
                                           partition_id=int(args[1]),
                                           broker_id=int(args[2])).count() == 0:
                tpbentry = TPBMapModel(topic_name=args[0],
                                       partition_id=int(args[1]),
                                       broker_id=int(args[2]))
                db.session.add(tpbentry)
                db.session.commit()
        else:
            pass
    elif operation == UPDATE:
        if table == "Consumer":
            consumer = ConsumerModel.query.filter_by(consumer_id=int(args[0]),
                                             topic_name=args[1]).first()
            if consumer.idx_read_upto < int(args[2]):
                consumer.idx_read_upto = int(args[2])
                db.session.commit()
        else:
            pass

def walchk():
    """
        1. Start reading WAL from the beginning, into a stack
        2. Status = BEGIN --> PUSH
           Status = END or ERR --> POP
        3. At the end of the reading all logs, the stack contains failed messages
        4. Add those to the respective DB tables
    """
    all_queries = WALModel.query.all()
    dict_uncommit_log = {}
    for query in all_queries:
        if(query)

