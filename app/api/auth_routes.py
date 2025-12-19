from __future__ import annotations

from fastapi import APIRouter, Depends, HTTPException, Response, status

from app.api.schemas import LoginRequest, LoginResponse, MeResponse, SignupRequest, SignupResponse
from app.auth.constants import AUTH_COOKIE_NAME
from app.auth.deps import get_current_user
from app.auth.security import create_access_token, hash_password, verify_password
from app.config.settings import settings
from app.db.crud_users import create_user, get_user_by_email, get_user_by_id
from app.db.models import User
from app.db.session import db_session

router = APIRouter(prefix="/auth", tags=["auth"])


@router.post("/signup", response_model=SignupResponse, status_code=201)
def signup(req: SignupRequest):
    with db_session() as db:
        existing = get_user_by_email(db, req.email)
        if existing:
            raise HTTPException(status_code=409, detail="Email already registered")

        user = create_user(db, email=req.email, password_hash=hash_password(req.password))
        db.commit()
        return SignupResponse(id=str(user.id), email=user.email)


@router.post("/login", response_model=LoginResponse)
def login(req: LoginRequest, response: Response):
    with db_session() as db:
        user = get_user_by_email(db, req.email)
        if not user or not verify_password(req.password, user.password_hash):
            raise HTTPException(
                status_code=status.HTTP_401_UNAUTHORIZED,
                detail="Invalid email or password",
            )

        token = create_access_token(subject=str(user.id))
        response.set_cookie(
            key=AUTH_COOKIE_NAME,
            value=token,
            httponly=True,
            samesite="lax",
            secure=False,  # set True when behind HTTPS
            max_age=int(60 * settings.jwt_expires_min),
            path="/",
        )
        return LoginResponse(access_token=token)


@router.get("/me", response_model=MeResponse)
def me(user: User = Depends(get_current_user)):
    return MeResponse(id=str(user.id), email=user.email)


@router.post("/logout")
def logout(response: Response):
    response.delete_cookie(key=AUTH_COOKIE_NAME, path="/")
    return {"ok": True}
