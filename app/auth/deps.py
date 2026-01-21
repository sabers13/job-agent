from __future__ import annotations

import uuid

from fastapi import Depends, HTTPException, Request
from fastapi.security import HTTPAuthorizationCredentials, HTTPBearer
from sqlalchemy.exc import DBAPIError, OperationalError

from app.auth.constants import AUTH_COOKIE_NAME
from app.auth.security import decode_token
from app.db.session import is_transient_db_error, run_db_with_retries
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

    def _op(db):
        return get_user_by_id(db, user_id)

    try:
        user = run_db_with_retries(_op, max_retries=2, base_sleep=0.4)
    except (OperationalError, DBAPIError) as exc:
        if is_transient_db_error(exc):
            raise HTTPException(
                status_code=503,
                detail={
                    "ok": False,
                    "transient": True,
                    "error_type": exc.__class__.__name__,
                    "error": (str(exc)[:300] if str(exc) else ""),
                },
            )
        raise
    if not user:
        raise HTTPException(status_code=401, detail="User not found")
    return user
