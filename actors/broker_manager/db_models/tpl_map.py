from src import db
from db_models import TopicModel, ProducerModel


class TPLMapModel(db.Model):
    """
    Stores entries as mapping of {TopicName(T)--ProducerID+PartitionID(P)--LogIndex(L)}
    """

    __tablename__ = "tpl_map"

    topic_name = db.Column(db.String, primary_key=True)
    producer_id = db.Column(db.Integer)
    partition_id = db.Column(db.Integer, nullable=False)
    log_index = db.Column(db.Integer, primary_key = True)
    updated_at = db.Column(db.DateTime, server_default=db.func.now(), server_onupdate=db.func.now(), nullable=False)

    __table_args__ = (
        db.UniqueConstraint("topic_name", "log_index", name="log_id_constraint"),
        db.ForeignKeyConstraint(["topic_name", "producer_id"], [ProducerModel.topic, ProducerModel.producer_id])
    )