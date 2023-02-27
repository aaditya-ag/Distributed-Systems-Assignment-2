from src import db

class TopicModel(db.Model):
    __tablename__ = 'topic'

    name = db.Column(db.String(64), primary_key=True)