from src import db

class WALModel(db.Model):
    __tablename__ = "wal"
    id = db.Column(db.Integer, primary_key=True)
    begin_id = db.Column(db.Integer, nullable=False, default=-1)
    operation = db.Column(db.Integer, nullable=False)
    table = db.Column(db.String, nullable=False)
    num_args = db.Column(db.Integer, nullable=False)
    args = db.Column(db.String, nullable=False)
    stage = db.Column(db.Integer, nullable=False)
    
    def as_dict(self):
        return {
            "id": self.id,
            "begin_id": self.begin_id,
            "operation": self.operation,
            "table": self.table,
            "num_args": self.num_args,
            "args": self.args,
            "stage": self.stage,
            
        }
