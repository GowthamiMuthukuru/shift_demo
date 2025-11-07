from sqlalchemy.orm import Session
from models.models import Users
from schemas.userschema import UserCreate, UserResponse
from fastapi import HTTPException, status
from utils.security import create_access_token, create_refresh_token, decode_refresh_token
import bcrypt


# PASSWORD HASHING UTILITIES
def hash_password(password: str) -> str:
    password_bytes = password.strip().encode("utf-8")[:72]
    salt = bcrypt.gensalt()
    hashed = bcrypt.hashpw(password_bytes, salt)
    return hashed.decode("utf-8")


def verify_password(plain_password: str, hashed_password: str) -> bool:
    plain_bytes = plain_password.strip().encode("utf-8")[:72]
    return bcrypt.checkpw(plain_bytes, hashed_password.encode("utf-8"))


# USER REGISTRATION
def register_user(db: Session, user: UserCreate) -> UserResponse:
    existing_user = db.query(Users).filter(Users.email == user.email).first()
    if existing_user:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Email already registered"
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
    user = db.query(Users).filter(Users.email == email).first()
    if not user or not verify_password(password, user.password_hash):
        raise HTTPException(status_code=401, detail="Invalid credentials")

    access_token = create_access_token({"user_id": user.id})
    refresh_token = create_refresh_token({"user_id": user.id})

    return {
        "access_token": access_token,
        "refresh_token": refresh_token,
        "token_type": "bearer"
    }


# TOKEN REFRESH
def refresh_access_token(refresh_token: str):
    payload = decode_refresh_token(refresh_token)
    new_access_token = create_access_token({"user_id": payload["user_id"]})
    return {"access_token": new_access_token, "token_type": "bearer"}
