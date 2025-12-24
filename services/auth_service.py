"""Authentication services for user registration, login, and token refresh."""

from sqlalchemy.orm import Session
import bcrypt
from fastapi import HTTPException, status
from models.models import Users
from schemas.userschema import UserCreate, UserResponse
from utils.security import create_access_token, create_refresh_token, decode_refresh_token


# PASSWORD HASHING UTILITIES
def hash_password(password: str) -> str:
    """Hash a plain-text password using bcrypt."""
    password_bytes = password.strip().encode("utf-8")[:72]
    salt = bcrypt.gensalt()
    hashed = bcrypt.hashpw(password_bytes, salt)
    return hashed.decode("utf-8")


def verify_password(plain_password: str, hashed_password: str) -> bool:
    """Verify a plain-text password against a bcrypt hash."""
    plain_bytes = plain_password.strip().encode("utf-8")[:72]
    return bcrypt.checkpw(plain_bytes, hashed_password.encode("utf-8"))


# USER REGISTRATION
def register_user(db: Session, user: UserCreate) -> UserResponse:
    """Register a new user after validating email and username uniqueness."""
    existing_user = db.query(Users).filter(Users.email == user.email).first()
    existing_username = db.query(Users).filter(Users.username == user.username).first()
    if existing_user:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Email already registered"
        )
    if existing_username:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Username already registered"
        )

    hashed_password = hash_password(user.password)

    db_user = Users(
        username=user.username,
        email=user.email,
        password_hash=hashed_password
    )

    db.add(db_user)
    db.commit()
    db.refresh(db_user)

    return db_user


# USER AUTHENTICATION
def authenticate_user(db: Session, email: str, password: str):
    """Authenticate a user and return access and refresh tokens."""
    user = db.query(Users).filter(Users.email == email).first()
    if not user or not verify_password(password, user.password_hash):
        raise HTTPException(status_code=401, detail="Invalid credentials")

    access_token = create_access_token({"user_id": user.id})
    refresh_token = create_refresh_token({"user_id": user.id})

    return {
        "access_token": access_token,
        "refresh_token": refresh_token,
        "token_type": "bearer",
        "user_id": user.id
    }


# TOKEN REFRESH
def refresh_access_token(refresh_token: str):
    """Generate a new access token using a valid refresh token."""
    payload = decode_refresh_token(refresh_token)
    new_access_token = create_access_token({"user_id": payload["user_id"]})
    return {"access_token": new_access_token, "token_type": "bearer"}
