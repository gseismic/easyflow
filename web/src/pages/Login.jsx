import { useState } from "react";
import { post } from "../api";

export default function Login({ onLogin, initialError }) {
  const [username, setUsername] = useState("");
  const [password, setPassword] = useState("");
  const [busy, setBusy] = useState(false);
  const [error, setError] = useState(initialError || "");
  const submit = async (event) => {
    event.preventDefault();
    setBusy(true);
    setError("");
    try {
      onLogin(await post("/api/auth/login", { username, password }));
    } catch (reason) {
      setError(reason.message);
    } finally {
      setBusy(false);
    }
  };
  return (
    <main className="login-shell">
      <div className="login-card">
        <div className="login-brand">
          <span className="brand-mark">
            e<span>·</span>
          </span>
          <strong>easyflow</strong>
        </div>
        <div className="eyebrow">
          WORKFLOW CONTROL <span>／ 工作流管理</span>
        </div>
        <h1>欢迎回来</h1>
        <p className="subtitle">登录后查看调度状态与任务运行情况。</p>
        <form onSubmit={submit}>
          <label className="field">
            用户名
            <input
              required
              autoFocus
              autoComplete="username"
              value={username}
              onChange={(event) => setUsername(event.target.value)}
            />
          </label>
          <label className="field">
            密码
            <input
              required
              type="password"
              autoComplete="current-password"
              value={password}
              onChange={(event) => setPassword(event.target.value)}
            />
          </label>
          {error && (
            <div className="form-error" role="alert">
              {error}
            </div>
          )}
          <button className="button primary login-submit" disabled={busy}>
            {busy ? "正在登录…" : "登录"}
          </button>
        </form>
        <div className="login-help">
          <strong>首次使用？</strong>
          <span>在终端创建管理员账户，然后使用该账户登录。</span>
          <code>uv run easyflow users create admin --role admin</code>
        </div>
      </div>
      <div className="login-footnote">
        EASYFLOW <span>／ 简单而清晰的工作流</span>
      </div>
    </main>
  );
}
