"""
Pytest fixtures for FastAPI application testing.

This module provides database setup, authenticated and unauthenticated
test clients, and dependency overrides required for API integration tests.
"""

import datetime as _dt
import pytest
from fastapi.testclient import TestClient
from sqlalchemy import create_engine, event
from sqlalchemy.engine import Engine
from sqlalchemy.orm import sessionmaker

from main import app
from db import Base, get_db
from utils.dependencies import get_current_user


def to_char_sqlite(value, fmt):
    """
    SQLite-compatible replacement for PostgreSQL to_char.

    Supports:
    - YYYY-MM
    - YYYY
    - MM
    """
    if value is None:
        return None

    if isinstance(value, (_dt.date, _dt.datetime)):
        dt = _dt.datetime(value.year, value.month, getattr(value, "day", 1))
    else:
        try:
            dt = _dt.datetime.fromisoformat(str(value))
        except ValueError:
            try:
                dt = _dt.datetime.strptime(str(value)[:10], "%Y-%m-%d")
            except ValueError:
                return None

    fmt = (fmt or "").upper()

    if fmt == "YYYY-MM":
        return dt.strftime("%Y-%m")
    if fmt == "YYYY":
        return dt.strftime("%Y")
    if fmt == "MM":
        return dt.strftime("%m")
    return dt.strftime("%Y-%m-%d")


@event.listens_for(Engine, "connect")
def register_sqlite_functions(dbapi_connection, _):
    """
    Register SQLite functions required for SQLAlchemy queries.
    """
    try:
        dbapi_connection.create_function("to_char", 2, to_char_sqlite)
    except AttributeError:
        pass


class FakeUser:
    """
    Fake authenticated user used in API tests.
    """

    def __init__(self, user_id=1, username="test_user", email="test@mouritech.com"):
        self.id = user_id
        self.username = username
        self.email = email


SQLALCHEMY_DATABASE_URL = "sqlite:///./test.db"

engine = create_engine(
    SQLALCHEMY_DATABASE_URL,
    connect_args={"check_same_thread": False},
)

TESTING_SESSION_LOCAL = sessionmaker(
    autocommit=False,
    autoflush=False,
    bind=engine,
)


@pytest.fixture(scope="session", autouse=True)
def create_test_db():
    """
    Create and drop database tables for the test session.
    """
    Base.metadata.create_all(bind=engine)
    yield
    Base.metadata.drop_all(bind=engine)


@pytest.fixture()
def db_session():
    """
    Provide a transactional database session per test.
    """
    session = TESTING_SESSION_LOCAL()
    try:
        yield session
    finally:
        session.rollback()
        session.close()


@pytest.fixture()
def client(db_session):
    """
    Authenticated FastAPI test client.
    """

    def override_get_db():
        yield db_session

    def override_get_current_user():
        return FakeUser()

    app.dependency_overrides[get_db] = override_get_db
    app.dependency_overrides[get_current_user] = override_get_current_user

    with TestClient(app) as test_client:
        yield test_client

    app.dependency_overrides.clear()


@pytest.fixture()
def unauth_client(db_session):
    """
    Unauthenticated FastAPI test client.
    """

    def override_get_db():
        yield db_session

    app.dependency_overrides[get_db] = override_get_db

    with TestClient(app) as test_client:
        yield test_client

    app.dependency_overrides.clear()
