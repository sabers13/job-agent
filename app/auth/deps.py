from __future__ import annotations

import uuid

from fastapi import Depends, HTTPException, Request
from fastapi.security import HTTPAuthorizationCredentials, HTTPBearer

from app.auth.constants import AUTH_COOKIE_NAME
from app.auth.security import decode_token
from app.db.session import db_session
from app.db.crud_users import get_user_by_id

bearer = HTTPBearer(auto_error=False)


def get_current_user(
    request: Request,
    creds: HTTPAuthorizationCredentials | None = Depends(bearer),
):
    token: str | None = None

    # 1) API clients
    if creds is not None and creds.credentials:
        token = creds.credentials

    # 2) Browser clients
    if token is None:
        token = request.cookies.get(AUTH_COOKIE_NAME)

    if not token:
        raise HTTPException(status_code=401, detail="Missing token")

    try:
        payload = decode_token(token)
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
