import { useEffect, useState } from "react";
import { api, post, setCsrfToken, usePolling } from "./api";
import SchedulerStatus from "./components/SchedulerStatus";
import DagList from "./pages/DagList";
import DagDetail from "./pages/DagDetail";
import HistoryList from "./pages/HistoryList";
import HistoryRun from "./pages/HistoryRun";
import Login from "./pages/Login";

function currentRoute() {
  const [path, query = ""] = window.location.hash.split("?", 2);
  if (path === "#/history") return { name: "history" };
  if (path === "#/history/run") {
    const params = new URLSearchParams(query);
    const dagId = params.get("dag_id");
    const runId = params.get("run_id");
    return dagId && runId
      ? { name: "history-run", dagId, runId }
      : { name: "history" };
  }
  const match = path.match(/^#\/dags\/(.+)$/);
  try {
    return match
      ? { name: "dag", dagId: decodeURIComponent(match[1]) }
      : { name: "dags" };
  } catch {
    return { name: "dags" };
  }
}

function Workspace({ session, onLogout }) {
  const [route, setRoute] = useState(currentRoute);
  const [historyView, setHistoryView] = useState({
    query: "",
    state: "",
    cursors: [null],
    page: 0,
  });
  const [logoutError, setLogoutError] = useState("");
  const overview = usePolling("/api/dags", 5000);
  useEffect(() => {
    const update = () => setRoute(currentRoute());
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
        <nav className="main-nav" aria-label="主导航">
          <a
            href="#/"
            aria-current={["dags", "dag"].includes(route.name) ? "page" : undefined}
          >
            工作流
          </a>
          <a
            href="#/history"
            aria-current={route.name.startsWith("history") ? "page" : undefined}
          >
            运行历史
          </a>
        </nav>
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
      {route.name === "dag" ? (
        <DagDetail
          key={route.dagId}
          dagId={route.dagId}
          refreshOverview={overview.refresh}
          isAdmin={session.role === "admin"}
        />
      ) : route.name === "history" ? (
        <HistoryList view={historyView} setView={setHistoryView} />
      ) : route.name === "history-run" ? (
        <HistoryRun
          key={JSON.stringify([route.dagId, route.runId])}
          dagId={route.dagId}
          runId={route.runId}
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
