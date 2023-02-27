from src import db
from db_models import BrokerModel


class BrokerSocketModel(db.Model):
    __tablename__ = "broker_socket"

    broker_id = db.Column(db.Integer, db.ForeignKey(BrokerModel.id), primary_key=True)
    ip = db.Column(db.String, nullable=False)
    port = db.Column(db.String, nullable=False)
