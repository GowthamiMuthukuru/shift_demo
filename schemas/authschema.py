"""
Authentication request schemas.

This module defines Pydantic models used for authentication-related
API requests such as user login and access token refresh.
"""

from pydantic import BaseModel
class LoginRequest(BaseModel):
    """
    Login request payload.

    Attributes:
        email (str): Registered user email address.
        password (str): Plain-text user password.
    """
    email: str
    password: str

class RefreshTokenRequest(BaseModel):
    """
    Refresh token request payload.

    Attributes:
        refresh_token (str): Valid refresh token used to issue
            a new access token.
    """
    refresh_token: str
