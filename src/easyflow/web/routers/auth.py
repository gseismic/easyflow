from fastapi import APIRouter, HTTPException, Request, Response
from pydantic import BaseModel, Field

from easyflow.web.auth import COOKIE_NAME, SESSION_SECONDS, csrf_token

router = APIRouter(prefix="/api/auth")


class LoginRequest(BaseModel):
    username: str = Field(max_length=64)
    password: str = Field(max_length=1024)


@router.post("/login")
def login(body: LoginRequest, request: Request, response: Response):
    auth = request.app.state.auth
    host = request.client.host if request.client else "unknown"
    if not request.app.state.login_throttle.allow(body.username, host):
        raise HTTPException(status_code=429, detail="登录尝试过多，请稍后再试。")
    user = auth.verify(body.username, body.password)
    if user is None:
        raise HTTPException(status_code=401, detail="用户名或密码不正确。")
    request.app.state.login_throttle.clear_user(body.username)
    old_token = request.cookies.get(COOKIE_NAME)
    auth.delete_session(old_token)
    try:
        token = auth.create_session(user)
    except ValueError as exc:
        raise HTTPException(status_code=401, detail=str(exc)) from exc
    response.set_cookie(
        COOKIE_NAME,
        token,
        max_age=SESSION_SECONDS,
        httponly=True,
        samesite="strict",
        secure=request.url.scheme == "https",
        path="/",
    )
    response.headers["Cache-Control"] = "no-store"
    return {
        "username": user.username,
        "role": user.role,
        "csrf_token": csrf_token(token),
    }


@router.get("/session")
def session(request: Request, response: Response):
    response.headers["Cache-Control"] = "no-store"
    user = request.state.user
    return {
        "username": user.username,
        "role": user.role,
        "csrf_token": csrf_token(request.cookies[COOKIE_NAME]),
    }


@router.post("/logout")
def logout(request: Request, response: Response):
    request.app.state.auth.delete_session(request.cookies.get(COOKIE_NAME))
    response.delete_cookie(COOKIE_NAME, path="/")
    response.headers["Cache-Control"] = "no-store"
    return {"ok": True}
