from db_models import *
from src import db
from datetime import datetime
from sqlalchemy import func

#OPERATION
INSERT=0
UPDATE=1
DELETE=2

def insert(table, data):
    if "updated_at" in data:
        data["updated_at"] = datetime.fromisoformat(data["updated_at"])
    
    if table == "Topic":
        # check if topic already present; add topic;
        if TopicModel.query.filter_by(name=data["name"]).first():
            return
        topic = TopicModel(**data)
        db.session.add(topic)
        db.session.commit()

    elif table == "Producer":
        # check if given entry already present; add producer;
        if ProducerModel.query.filter_by(producer_id=data["producer_id"], topic=data["topic"]).first():
            return 
        
        prod = ProducerModel(**data)
        db.session.add(prod)
        db.session.commit()

    elif table == "Consumer":
        # check if consumer already present; add consumer;
        if ConsumerModel.query.filter_by(consumer_id=data["consumer_id"], topic=data["topic"]).first():
            return
        cons = ConsumerModel(**data)
        db.session.add(cons)
        db.session.commit()

    elif table == "Broker":
        # check if broker already present; add broker;
        if BrokerModel.query.filter_by(id=data["id"]).first():
            return
        broker = BrokerModel(**data)
        db.session.add(broker)
        db.session.commit()
        
    elif table == "TPLMap":
        # check if entry already present; add entry;
        if TPLMapModel.query.filter_by(topic_name=data["topic_name"], log_index=data["log_index"]).first():
            return 
        tpl_entry = TPLMapModel(**data)
        db.session.add(tpl_entry)
        db.session.commit()

    elif table == "TPBMap":
        # check if entry already present; add entry;
        if TPBMapModel.query.filter_by(id=data["id"]).first():
            return
        tpb_entry = TPBMapModel(**data)
        db.session.add(tpb_entry)
        db.session.commit()
    else:
        pass


def update(table, data):
    if "updated_at" in data:
        data["updated_at"] = datetime.fromisoformat(data["updated_at"])
    
    if table == "Consumer":
        consumer = ConsumerModel.query.filter_by(consumer_id=data["consumer_id"], topic_name=data["topic_name"]).first()
        if consumer is None:
            return
        if consumer.idx_read_upto < data["idx_read_upto"]:
            consumer.idx_read_upto = data["idx_read_upto"]
            db.session.commit()

    # elif table == "Broker":
    #     pass
    else:
        pass
    
def sync(operation, table, data):
    """
        tablename = [Topic, Producer, Consumer, Broker, TPLMap, TBPMap]
        operation = [INSERT, UPDATE]
    """
    if operation == INSERT:
        insert(table, data)
    elif operation == UPDATE:
        update(table, data)
    elif operation == DELETE:
        pass
    else:
        pass

def get_minimum_of_max_timestamps_from_all_tables():
    """
    Returns the minimum of the maximum timestamps of all the tables
    """
    max_timestamps = []
    max_timestamps.append(db.session.query(func.max(TopicModel.updated_at)).scalar() or datetime.min)
    max_timestamps.append(db.session.query(func.max(ProducerModel.updated_at)).scalar() or datetime.min)
    max_timestamps.append(db.session.query(func.max(ConsumerModel.updated_at)).scalar() or datetime.min)
    max_timestamps.append(db.session.query(func.max(BrokerModel.updated_at)).scalar() or datetime.min)
    max_timestamps.append(db.session.query(func.max(TPLMapModel.updated_at)).scalar() or datetime.min)
    max_timestamps.append(db.session.query(func.max(TPBMapModel.updated_at)).scalar() or datetime.min)
    print(min(max_timestamps).isoformat())
    return min(max_timestamps).isoformat()