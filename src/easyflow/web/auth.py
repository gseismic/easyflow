"""本机 Web 的账户、服务端会话与登录限流。"""

from __future__ import annotations

import hashlib
import hmac
import re
import secrets
import sqlite3
import threading
import time
from collections import deque
from dataclasses import dataclass
from pathlib import Path

from argon2 import PasswordHasher
from argon2.exceptions import InvalidHashError, VerificationError

SESSION_SECONDS = 12 * 60 * 60
COOKIE_NAME = "easyflow_session"
_USERNAME = re.compile(r"^[A-Za-z0-9_.-]{1,64}$")
_PASSWORDS = PasswordHasher(time_cost=2, memory_cost=19456, parallelism=1)


@dataclass(frozen=True)
class User:
    username: str
    role: str
    credential_hash: str | None = None


class AuthStore:
    def __init__(self, path: Path) -> None:
        self.path = path
        self._dummy_hash = _PASSWORDS.hash(secrets.token_urlsafe(32))
        with self._connect() as connection:
            connection.executescript("""
                CREATE TABLE IF NOT EXISTS web_user (
                    username TEXT PRIMARY KEY,
                    password_hash TEXT NOT NULL,
                    role TEXT NOT NULL CHECK (role IN ('admin', 'viewer'))
                );
                CREATE TABLE IF NOT EXISTS web_session (
                    token_hash TEXT PRIMARY KEY,
                    username TEXT NOT NULL,
                    expires_at INTEGER NOT NULL,
                    FOREIGN KEY (username) REFERENCES web_user(username)
                );
            """)

    def _connect(self) -> sqlite3.Connection:
        connection = sqlite3.connect(self.path, timeout=30)
        connection.execute("PRAGMA foreign_keys=ON")
        return connection

    @staticmethod
    def _validate_password(password: str) -> None:
        if len(password) < 10 or len(password.encode("utf-8")) > 1024:
            raise ValueError("密码须至少 10 个字符，且不超过 1024 字节。")

    def create_user(self, username: str, password: str, role: str) -> None:
        if not _USERNAME.fullmatch(username):
            raise ValueError("用户名只允许 1–64 个字母、数字、点、下划线或连字符。")
        if role not in {"admin", "viewer"}:
            raise ValueError("角色必须是 admin 或 viewer。")
        self._validate_password(password)
        password_hash = _PASSWORDS.hash(password)
        try:
            with self._connect() as connection:
                connection.execute(
                    "INSERT INTO web_user(username, password_hash, role) VALUES (?, ?, ?)",
                    (username, password_hash, role),
                )
        except sqlite3.IntegrityError as exc:
            raise ValueError("用户名已存在。") from exc

    def set_password(self, username: str, password: str) -> None:
        self._validate_password(password)
        password_hash = _PASSWORDS.hash(password)
        with self._connect() as connection:
            changed = connection.execute(
                "UPDATE web_user SET password_hash = ? WHERE username = ?",
                (password_hash, username),
            ).rowcount
            if not changed:
                raise ValueError("用户不存在。")
            connection.execute(
                "DELETE FROM web_session WHERE username = ?", (username,)
            )

    def list_users(self) -> list[User]:
        with self._connect() as connection:
            return [
                User(*row)
                for row in connection.execute(
                    "SELECT username, role FROM web_user ORDER BY username"
                )
            ]

    def remove_user(self, username: str) -> None:
        with self._connect() as connection:
            connection.execute("BEGIN IMMEDIATE")
            row = connection.execute(
                "SELECT role FROM web_user WHERE username = ?", (username,)
            ).fetchone()
            if row is None:
                raise ValueError("用户不存在。")
            if (
                row[0] == "admin"
                and connection.execute(
                    "SELECT COUNT(*) FROM web_user WHERE role = 'admin'"
                ).fetchone()[0]
                <= 1
            ):
                raise ValueError("不能删除最后一个管理员。")
            connection.execute(
                "DELETE FROM web_session WHERE username = ?", (username,)
            )
            connection.execute("DELETE FROM web_user WHERE username = ?", (username,))

    def verify(self, username: str, password: str) -> User | None:
        if len(username) > 64 or len(password.encode("utf-8")) > 1024:
            return None
        with self._connect() as connection:
            row = connection.execute(
                "SELECT password_hash, role FROM web_user WHERE username = ?",
                (username,),
            ).fetchone()
        try:
            valid = _PASSWORDS.verify(row[0] if row else self._dummy_hash, password)
        except (VerificationError, InvalidHashError):
            valid = False
        return User(username, row[1], row[0]) if row and valid else None

    def create_session(self, user: User) -> str:
        token = secrets.token_urlsafe(32)
        now = int(time.time())
        with self._connect() as connection:
            connection.execute("BEGIN IMMEDIATE")
            current = connection.execute(
                "SELECT password_hash, role FROM web_user WHERE username = ?",
                (user.username,),
            ).fetchone()
            if not current or current != (user.credential_hash, user.role):
                raise ValueError("账户信息已变化，请重新登录。")
            connection.execute("DELETE FROM web_session WHERE expires_at <= ?", (now,))
            connection.execute(
                "INSERT INTO web_session(token_hash, username, expires_at) VALUES (?, ?, ?)",
                (_token_hash(token), user.username, now + SESSION_SECONDS),
            )
        return token

    def session_user(self, token: str | None) -> User | None:
        if not token or len(token) > 128:
            return None
        with self._connect() as connection:
            row = connection.execute(
                "SELECT u.username, u.role FROM web_session s "
                "JOIN web_user u ON u.username = s.username "
                "WHERE s.token_hash = ? AND s.expires_at > ?",
                (_token_hash(token), int(time.time())),
            ).fetchone()
        return User(*row) if row else None

    def delete_session(self, token: str | None) -> None:
        if token and len(token) <= 128:
            with self._connect() as connection:
                connection.execute(
                    "DELETE FROM web_session WHERE token_hash = ?",
                    (_token_hash(token),),
                )


def _token_hash(token: str) -> str:
    return hashlib.sha256(token.encode("utf-8")).hexdigest()


def csrf_token(token: str) -> str:
    return hmac.new(
        token.encode("utf-8"), b"easyflow-csrf-v1", hashlib.sha256
    ).hexdigest()


class LoginThrottle:
    """同一进程内限制单账户和单客户端的连续登录尝试。"""

    def __init__(self) -> None:
        self._lock = threading.Lock()
        self._attempts: dict[str, deque[float]] = {}

    def allow(self, username: str, host: str) -> bool:
        now = time.monotonic()
        keys = ((f"user:{username}", 6), (f"host:{host}", 30))
        with self._lock:
            for key in list(self._attempts):
                attempts = self._attempts[key]
                while attempts and attempts[0] <= now - 60:
                    attempts.popleft()
                if not attempts:
                    del self._attempts[key]
            if any(len(self._attempts.get(key, ())) >= limit for key, limit in keys):
                return False
            for key, _ in keys:
                self._attempts.setdefault(key, deque()).append(now)
        return True

    def clear_user(self, username: str) -> None:
        with self._lock:
            self._attempts.pop(f"user:{username}", None)
