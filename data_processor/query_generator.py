import os
from sqlalchemy import create_engine, Column, String, Integer, and_, Boolean, func, text
from sqlalchemy.orm import declarative_base, sessionmaker

Base = declarative_base()

class CorpusQuery(Base):
    __tablename__ = 'corpus_query'
    corpus_id = Column(String, primary_key=True)
    corpus_type = Column(String, primary_key=True)
    is_generated = Column(Integer, default=0)

def init_db(db_path):
    engine = create_engine(f'sqlite:///{db_path}')
    Base.metadata.create_all(engine)
    Session = sessionmaker(bind=engine)
    return Session

def save_corpus_query(db_path, corpus_id, corpus_type):
    """
    保存 corpus_id, corpus_type, is_generated=1 到数据库
    """
    Session = init_db(db_path)
    session = Session()
    obj = CorpusQuery(corpus_id=corpus_id, corpus_type=corpus_type, is_generated=1)
    session.merge(obj)
    session.commit()
    session.close()


if __name__ == "__main__":
    # Example usage
    output_dir = os.path.join(os.path.dirname(__file__), '../temp_output/smartcn')
    db_path = os.path.join(output_dir, 'textbooks.db')
    save_corpus_query(db_path, '0ae9dd52-c5c7-9e1a-df73-4408afda53ed', 'lesson_plan')
    print("Corpus query saved successfully.")
