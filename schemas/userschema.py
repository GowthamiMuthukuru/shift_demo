from pydantic import BaseModel, EmailStr, field_validator,Field

# BASE USER MODEL
class UserBase(BaseModel):
    username: str
    email: EmailStr = Field(..., example="user@mouritech.com")

    @field_validator("username")
    @classmethod
    def validate_username(cls, v):
        if not (3 <= len(v) <= 100):
            raise ValueError("Username must be between 3 and 100 characters")
        return v

    @field_validator("email")
    @classmethod
    def validate_email_domain(cls, v):
        allowed_domains = ("mouritech.com")
        domain = v.split("@")[-1].lower()
        if domain not in allowed_domains:
            raise ValueError("Email domain must be of mouritech")
        return v


# USER CREATION SCHEMA
class UserCreate(UserBase):
    password: str

    @field_validator("password")
    @classmethod
    def validate_password(cls, v):
        if not (8 <= len(v) <= 72):
            raise ValueError("Password must be between 8 and 72 characters")
        return v


# RESPONSE SCHEMA (For returning user info)
class UserResponse(BaseModel):
    id: int
    username: str
    email: str

    class Config:
        from_attributes = True 
