from src import db


class TopicModel(db.Model):
    __tablename__ = "topic"

    name = db.Column(db.String, primary_key=True)
