import { useEffect, useRef, useState } from "react";
import { api, post, setCsrfToken, usePolling } from "./api";
import SchedulerStatus from "./components/SchedulerStatus";
import DagList from "./pages/DagList";
import DagDetail from "./pages/DagDetail";
import HistoryList from "./pages/HistoryList";
import HistoryRun from "./pages/HistoryRun";
import Login from "./pages/Login";

function currentRoute() {
  const [path, query = ""] = window.location.hash.split("?", 2);
  if (path === "#/history") {
    return { name: "history", dagId: new URLSearchParams(query).get("dag_id") || "" };
  }
  if (path === "#/history/run") {
    const params = new URLSearchParams(query);
    const dagId = params.get("dag_id");
    const runId = params.get("run_id");
    return dagId && runId
      ? { name: "history-run", dagId, runId, backDagId: params.get("from_dag") || "" }
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

const emptyFilters = () => ({ projectKind: "all", project: "", tags: [] });

function Workspace({ session, onLogout }) {
  const [route, setRoute] = useState(currentRoute);
  const historyScope = useRef(route.name === "history" ? route.dagId : "");
  const [filters, setFilters] = useState(emptyFilters);
  const [workflowView, setWorkflowView] = useState({
    query: "",
    draft: "",
    resetVersion: 0,
    state: "all",
    cursors: [null],
    page: 0,
  });
  const [historyView, setHistoryView] = useState({
    query: "",
    draft: "",
    resetVersion: 0,
    state: "",
    cursors: [null],
    page: 0,
  });
  const [logoutError, setLogoutError] = useState("");
  const overview = usePolling("/api/catalog", 5000);
  useEffect(() => {
    const update = () => setRoute(currentRoute());
    window.addEventListener("hashchange", update);
    return () => window.removeEventListener("hashchange", update);
  }, []);
  useEffect(() => {
    if (route.name !== "history") return;
    const scope = route.dagId || "";
    if (historyScope.current !== scope) {
      historyScope.current = scope;
      setHistoryView((current) => ({ ...current, cursors: [null], page: 0 }));
    }
  }, [route.name, route.dagId]);
  const logout = async () => {
    try {
      await post("/api/auth/logout", {});
      onLogout();
    } catch (error) {
      setLogoutError(error.message);
    }
  };
  const changeFilters = (next) => {
    setFilters(next);
    setWorkflowView((current) => ({ ...current, cursors: [null], page: 0 }));
    setHistoryView((current) => ({ ...current, cursors: [null], page: 0 }));
  };
  const resetFilters = (page) => {
    setFilters(emptyFilters());
    setWorkflowView((current) => ({
      ...current,
      ...(page === "dags" ? {
        query: "", draft: "", state: "all", resetVersion: current.resetVersion + 1,
      } : {}),
      cursors: [null], page: 0,
    }));
    setHistoryView((current) => ({
      ...current,
      ...(page === "history" ? {
        query: "", draft: "", state: "", resetVersion: current.resetVersion + 1,
      } : {}),
      cursors: [null], page: 0,
    }));
    if (page === "history" && route.name === "history" && route.dagId) {
      window.location.hash = "#/history";
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
          onViewAllRuns={() => resetFilters("history")}
        />
      ) : route.name === "history" ? (
        <HistoryList
          view={historyView}
          setView={setHistoryView}
          filters={filters}
          onFiltersChange={changeFilters}
          onReset={() => resetFilters("history")}
          dagId={route.dagId}
          catalog={overview}
        />
      ) : route.name === "history-run" ? (
        <HistoryRun
          key={JSON.stringify([route.dagId, route.runId])}
          dagId={route.dagId}
          runId={route.runId}
          backDagId={route.backDagId}
        />
      ) : (
        <DagList
          overview={overview}
          view={workflowView}
          setView={setWorkflowView}
          filters={filters}
          onFiltersChange={changeFilters}
          onReset={() => resetFilters("dags")}
        />
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
