import secrets
from datetime import datetime, timedelta
from typing import Optional
from fastapi import HTTPException, Header

# Simple in-memory token store for demo
TOKENS = {}

# Hardcoded valid credentials
VALID_CREDENTIALS = {
    "admin": "password"
}

def create_token(username: str) -> str:
    token = secrets.token_hex(16)
    expiration = datetime.utcnow() + timedelta(hours=1)
    TOKENS[token] = {"username": username, "expires": expiration}
    return token

def validate_token(token: str) -> bool:
    if not token:
        return False
    
    session = TOKENS.get(token)
    if not session:
        return False
        
    if datetime.utcnow() > session["expires"]:
        del TOKENS[token]
        return False
        
    return True

async def get_current_user(x_auth_token: Optional[str] = Header(None)):
    if not x_auth_token or not validate_token(x_auth_token):
        raise HTTPException(status_code=401, detail="Invalid or expired token")
    return TOKENS[x_auth_token]["username"]
