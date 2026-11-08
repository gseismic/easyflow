from __future__ import annotations

import signal
import hmac
import subprocess
import sys
from contextlib import asynccontextmanager
from pathlib import Path
from urllib.parse import urlsplit

import uvicorn
from fastapi import FastAPI, Request
from fastapi.responses import FileResponse, JSONResponse
from fastapi.staticfiles import StaticFiles
from starlette.middleware.trustedhost import TrustedHostMiddleware

from easyflow.sdk.exceptions import StateConflictError
from easyflow.web.auth import AuthStore, COOKIE_NAME, LoginThrottle, csrf_token
from easyflow.web.routers import auth, dags, history, runs
from easyflow.web.service import WebService

STATIC = Path(__file__).parent / "static"


def create_app(*, dags_folder: str, database_url: str, log_root: str) -> FastAPI:
    service = WebService(dags_folder, database_url, log_root)

    @asynccontextmanager
    async def lifespan(app: FastAPI):
        yield
        service.discovery.close()

    app = FastAPI(
        title="easyflow",
        docs_url=None,
        redoc_url=None,
        openapi_url=None,
        lifespan=lifespan,
    )
    app.state.service = service
    app.state.auth = AuthStore(service.store.path)
    app.state.login_throttle = LoginThrottle()
    app.add_middleware(TrustedHostMiddleware, allowed_hosts=["localhost", "127.0.0.1"])

    @app.middleware("http")
    async def same_origin(request: Request, call_next):
        origin = request.headers.get("origin")
        if request.method in {"POST", "PUT", "PATCH", "DELETE"} and origin:
            parsed = urlsplit(origin)
            if (
                parsed.scheme != request.url.scheme
                or parsed.netloc != request.headers.get("host")
            ):
                return JSONResponse(
                    {"detail": "请求来源与本机界面不一致。"}, status_code=403
                )
        if (
            request.url.path.startswith("/api/")
            and request.url.path != "/api/auth/login"
        ):
            token = request.cookies.get(COOKIE_NAME)
            user = app.state.auth.session_user(token)
            if user is None:
                return JSONResponse({"detail": "请先登录。"}, status_code=401)
            request.state.user = user
            if request.method in {"POST", "PUT", "PATCH", "DELETE"}:
                supplied = request.headers.get("x-csrf-token", "")
                if not hmac.compare_digest(
                    supplied.encode(), csrf_token(token).encode()
                ):
                    return JSONResponse(
                        {"detail": "请求校验失败，请刷新页面。"}, status_code=403
                    )
                if user.role != "admin" and request.url.path != "/api/auth/logout":
                    return JSONResponse({"detail": "需要管理员权限。"}, status_code=403)
        response = await call_next(request)
        if request.url.path.startswith("/api/"):
            response.headers["Cache-Control"] = "no-store"
        return response

    @app.exception_handler(LookupError)
    async def not_found(request: Request, exc: LookupError):
        return JSONResponse({"detail": str(exc)}, status_code=404)

    @app.exception_handler(ValueError)
    @app.exception_handler(StateConflictError)
    async def invalid_request(request: Request, exc: Exception):
        return JSONResponse({"detail": str(exc)}, status_code=400)

    app.include_router(auth.router)
    app.include_router(dags.router)
    app.include_router(history.router)
    app.include_router(runs.router)
    app.mount(
        "/assets",
        StaticFiles(directory=STATIC / "assets", check_dir=False),
        name="assets",
    )

    @app.get("/", include_in_schema=False)
    def index():
        if not (STATIC / "index.html").is_file():
            return JSONResponse(
                {
                    "detail": "界面尚未构建，请在 web 目录运行 pnpm install 和 pnpm build。"
                },
                status_code=503,
            )
        return FileResponse(STATIC / "index.html")

    return app


def serve(args) -> int:
    if not 1 <= args.port <= 65535:
        print("port 必须在 1 到 65535 之间。", file=sys.stderr)
        return 2
    Path(args.dags_folder).mkdir(parents=True, exist_ok=True)
    app = create_app(
        dags_folder=args.dags_folder,
        database_url=args.database_url,
        log_root=args.log_root,
    )
    service = app.state.service
    if args.parallelism is not None:
        try:
            service.store.configure_parallelism(args.parallelism, override=True)
        except ValueError as exc:
            print(str(exc), file=sys.stderr)
            return 2
    scheduler = None
    log_handle = None
    if not args.no_scheduler and not service.scheduler_running():
        log_path = service.store.path.resolve().parent / "scheduler.log"
        log_handle = log_path.open("ab")
        command = [
            sys.executable,
            "-m",
            "easyflow.cli",
            "scheduler",
            "--dags-folder",
            args.dags_folder,
            "--database-url",
            args.database_url,
            "--log-root",
            args.log_root,
            "--interval-seconds",
            "2",
            "--executor",
            "local",
        ]
        scheduler = subprocess.Popen(
            command, stdout=log_handle, stderr=subprocess.STDOUT
        )
    print(f"easyflow 管理界面：http://127.0.0.1:{args.port}", flush=True)
    try:
        uvicorn.run(app, host="127.0.0.1", port=args.port, log_level="warning")
    finally:
        if scheduler is not None and scheduler.poll() is None:
            scheduler.send_signal(signal.SIGINT)
            try:
                scheduler.wait(timeout=10)
            except subprocess.TimeoutExpired:
                scheduler.terminate()
                scheduler.wait(timeout=5)
        if log_handle is not None:
            log_handle.close()
    return 0
