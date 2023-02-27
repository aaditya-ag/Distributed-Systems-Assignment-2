from src import db


class BrokerModel(db.Model):
    __tablename__ = "broker"

    id = db.Column(db.Integer, primary_key=True)
    ip = db.Column(db.String, nullable=False)
    port = db.Column(db.String, nullable=False)
    num_partitions = db.Column(db.Integer, nullable=False)
    status = db.Column(db.Boolean, default=False, nullable=False)

    def as_dict(self):
        return {
            "id": self.id,
            "ip": self.ip,
            "port": self.port,
            "num_partitions": self.num_partitions,
            "status": self.status,
        }
