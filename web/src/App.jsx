import { useEffect, useState } from "react";
import { api, post, setCsrfToken, usePolling } from "./api";
import SchedulerStatus from "./components/SchedulerStatus";
import DagList from "./pages/DagList";
import DagDetail from "./pages/DagDetail";
import Login from "./pages/Login";

function currentDag() {
  const match = window.location.hash.match(/^#\/dags\/(.+)$/);
  try {
    return match ? decodeURIComponent(match[1]) : null;
  } catch {
    return null;
  }
}

function Workspace({ session, onLogout }) {
  const [dagId, setDagId] = useState(currentDag);
  const [logoutError, setLogoutError] = useState("");
  const overview = usePolling("/api/dags", 5000);
  useEffect(() => {
    const update = () => setDagId(currentDag());
    window.addEventListener("hashchange", update);
    return () => window.removeEventListener("hashchange", update);
  }, []);
  const logout = async () => {
    try {
      await post("/api/auth/logout", {});
      onLogout();
    } catch (error) {
      setLogoutError(error.message);
    }
  };
  return (
    <>
      <header className="topbar">
        <a href="#/" className="brand" aria-label="easyflow 首页">
          <span className="brand-mark">
            e<span>·</span>
          </span>
          <strong>easyflow</strong>
        </a>
        <span className="header-divider" />
        <span className="header-label">工作流管理</span>
        <div className="topbar-spacer" />
        <SchedulerStatus overview={overview} />
        <div className="user-menu">
          <span title={session.username}>
            {session.username} · {session.role === "admin" ? "管理员" : "只读"}
          </span>
          <button className="link-button" onClick={logout}>
            退出
          </button>
        </div>
      </header>
      {logoutError && (
        <div className="notice error" role="alert">
          退出失败：{logoutError}
        </div>
      )}
      {dagId ? (
        <DagDetail
          key={dagId}
          dagId={dagId}
          refreshOverview={overview.refresh}
          isAdmin={session.role === "admin"}
        />
      ) : (
        <DagList overview={overview} />
      )}
    </>
  );
}

export default function App() {
  const [session, setSession] = useState(undefined);
  const [startupError, setStartupError] = useState("");
  useEffect(() => {
    let active = true;
    const expire = () => {
      setCsrfToken("");
      if (active) setSession(null);
    };
    window.addEventListener("easyflow:unauthorized", expire);
    api("/api/auth/session")
      .then((value) => {
        if (!active) return;
        setCsrfToken(value.csrf_token);
        setSession(value);
      })
      .catch((error) => {
        if (!active) return;
        if (error.status !== 401)
          setStartupError(`连接服务失败：${error.message}`);
        setSession(null);
      });
    return () => {
      active = false;
      window.removeEventListener("easyflow:unauthorized", expire);
    };
  }, []);
  const authenticated = (value) => {
    setCsrfToken(value.csrf_token);
    setSession(value);
  };
  if (session === undefined)
    return (
      <main className="login-shell">
        <div className="empty">正在连接工作台…</div>
      </main>
    );
  if (!session)
    return <Login initialError={startupError} onLogin={authenticated} />;
  return (
    <Workspace
      session={session}
      onLogout={() => {
        setCsrfToken("");
        setSession(null);
      }}
    />
  );
}
