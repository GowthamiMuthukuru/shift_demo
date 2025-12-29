"""
Authentication dependency utilities.

This module provides reusable FastAPI dependencies related to authentication,
including extraction and validation of the current authenticated user from
a bearer access token.
"""

from fastapi import Depends, HTTPException, status
from fastapi.security import HTTPBearer, HTTPAuthorizationCredentials
from sqlalchemy.orm import Session
from db import get_db
from utils.security import decode_access_token
from models.models import Users


security = HTTPBearer()


def get_current_user(
    credentials: HTTPAuthorizationCredentials = Depends(security),
    db: Session = Depends(get_db),
):
    """
    Retrieve the currently authenticated user.

    This dependency extracts the bearer token from the request, decodes
    the access token, and validates that the corresponding user exists
    in the database.

    Args:
        credentials (HTTPAuthorizationCredentials): Bearer token credentials.
        db (Session): Active SQLAlchemy database session.

    Returns:
        Users: Authenticated user ORM instance.

    Raises:
        HTTPException:
            - 401 if the token is invalid or the user does not exist.
    """
    token = credentials.credentials
    payload = decode_access_token(token)

    user = db.query(Users).filter(Users.id == payload["user_id"]).first()
    if not user:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="User not found"
        )

    return user
