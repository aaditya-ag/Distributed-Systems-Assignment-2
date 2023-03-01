from src import db


class BrokerModel(db.Model):
    __tablename__ = "broker"

    id = db.Column(db.Integer, primary_key=True)
    ip = db.Column(db.String, nullable=False)
    port = db.Column(db.String, nullable=False)
    is_running = db.Column(db.Boolean, default=True, nullable=False)

    def as_dict(self):
        return {
            "id": self.id,
            "ip": self.ip,
            "port": self.port,
            "is_running": self.is_running,
        }
