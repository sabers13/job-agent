from __future__ import annotations

import uuid

from fastapi import Depends, HTTPException
from fastapi.security import HTTPAuthorizationCredentials, HTTPBearer

from app.auth.security import decode_token
from app.db.session import db_session
from app.db.crud_users import get_user_by_id

bearer = HTTPBearer(auto_error=False)


def get_current_user(creds: HTTPAuthorizationCredentials | None = Depends(bearer)):
    if creds is None or not creds.credentials:
        raise HTTPException(status_code=401, detail="Missing bearer token")

    try:
        payload = decode_token(creds.credentials)
        sub = payload.get("sub")
        if not sub:
            raise HTTPException(status_code=401, detail="Token missing subject")
        user_id = uuid.UUID(sub)
    except HTTPException:
        raise
    except Exception:
        raise HTTPException(status_code=401, detail="Invalid token")

    with db_session() as db:
        user = get_user_by_id(db, user_id)
        if not user:
            raise HTTPException(status_code=401, detail="User not found")
        return user
