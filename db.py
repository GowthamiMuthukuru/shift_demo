from sqlalchemy.orm import sessionmaker, declarative_base
from sqlalchemy import create_engine
from dotenv import load_dotenv
import os
load_dotenv()
DATABASE_URL = os.getenv("DB_URL")
Base = declarative_base()
engine = create_engine(DATABASE_URL)
Session = sessionmaker(autocommit=False, autoflush=False, bind=engine)
def get_db():
    db = Session()
    try:
        yield db
    finally:
        db.close()
