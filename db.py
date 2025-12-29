"""
Database configuration and session management.

This module initializes the SQLAlchemy engine, session factory,
and declarative base. It also provides a database dependency
for FastAPI routes to safely acquire and release sessions.
"""

import os
from sqlalchemy.orm import sessionmaker, declarative_base
from sqlalchemy import create_engine
from dotenv import load_dotenv

load_dotenv()

DATABASE_URL = os.getenv("DB_URL")

if not DATABASE_URL:
    raise ValueError("DATABASE_URL not found! Check your .env file.")

Base = declarative_base()
engine = create_engine(DATABASE_URL)

Session = sessionmaker(autocommit=False, autoflush=False, bind=engine)

def get_db():
    """
    Provide a database session dependency.

    Yields:
        Session: SQLAlchemy database session.

    Ensures the session is properly closed after use.
    """
    db = Session()
    try:
        yield db
    finally:
        db.close()
