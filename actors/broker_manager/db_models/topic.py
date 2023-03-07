from src import db


class TopicModel(db.Model):
    __tablename__ = "topic"

    name = db.Column(db.String, primary_key=True)
    
    updated_at = db.Column(db.DateTime, server_default=db.func.now(), server_onupdate=db.func.now(), nullable=False)

    def as_dict(self):
        return {
            "name": self.name,
            "updated_at": self.updated_at.isoformat()
        }