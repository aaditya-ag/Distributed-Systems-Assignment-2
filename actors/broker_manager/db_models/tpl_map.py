from src import db
from db_models import TopicModel, ProducerModel


class TPLMapModel(db.Model):
    """
    Stores entries as mapping of {TopicName(T)--PartitionID(P)--LogIndex(L)}
    """

    __tablename__ = "tpl_map"

    id = db.Column(db.Integer, primary_key=True)
    topic_name = db.Column(db.String, db.ForeignKey(TopicModel.name))
    producer_id = db.Column(db.Integer, db.ForeignKey(ProducerModel.producer_id))
    partition_id = db.Column(db.Integer, nullable=False)
    log_index = db.Column(db.Integer, nullable=False)
