import { useEffect, useRef, useState } from "react";
import { api, post, setCsrfToken, usePolling } from "./api";
import SchedulerStatus from "./components/SchedulerStatus";
import usePendingRefresh from "./components/usePendingRefresh";
import DagList from "./pages/DagList";
import DagDetail from "./pages/DagDetail";
import HistoryList from "./pages/HistoryList";
import HistoryRun from "./pages/HistoryRun";
import Login from "./pages/Login";
import { emptyFilters, listHref, readListState, sameFilters, workflowHref } from "./listRoute";

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
      ? {
          name: "history-run", dagId, runId,
          backDagId: params.get("from_dag") || "",
          fromWorkflow: params.get("from_workflow") === "1",
          backQuery: (params.get("from_q") || "").slice(0, 1000).trim(),
          backState: params.get("from_state") || "",
        }
      : { name: "history" };
  }
  const match = path.match(/^#\/dags\/(.+)$/);
  try {
    return match
      ? {
          name: "dag", dagId: decodeURIComponent(match[1]),
          tab: ["history", "schedule", "source"].includes(new URLSearchParams(query).get("tab"))
            ? new URLSearchParams(query).get("tab") : "run",
        }
      : { name: "dags" };
  } catch {
    return { name: "dags" };
  }
}

function makeView(parsed) {
  return {
    query: parsed.query,
    draft: parsed.query,
    draftComposing: false,
    resetVersion: 0,
    filterResetVersion: 0,
    state: parsed.state,
    cursors: [null],
    page: 0,
  };
}

function initialView(page) {
  const currentPage = currentRoute().name;
  return makeView(readListState(currentPage === page ? window.location.hash : "", page));
}

function Workspace({ session, onLogout }) {
  const [route, setRoute] = useState(currentRoute);
  const historyScope = useRef(route.name === "history" ? route.dagId : "");
  const [filters, setFilters] = useState(() => {
    const page = currentRoute().name;
    return page === "dags" || page === "history"
      ? readListState(window.location.hash, page).filters : emptyFilters();
  });
  const [workflowView, setWorkflowView] = useState(() => initialView("dags"));
  const [historyView, setHistoryView] = useState(() => initialView("history"));
  const [dagHistory, setDagHistory] = useState(() => ({
    dagId: route.name === "dag" ? route.dagId : "",
    view: makeView(readListState(
      route.name === "dag" && route.tab === "history" ? window.location.hash : "",
      "history",
    )),
  }));
  const filtersRef = useRef(filters);
  filtersRef.current = filters;
  const [logoutError, setLogoutError] = useState("");
  const overview = usePolling("/api/catalog", 5000);
  usePendingRefresh(overview, overview.data?.loading);
  useEffect(() => {
    const update = () => {
      const nextRoute = currentRoute();
      if (nextRoute.name === "dags" || nextRoute.name === "history") {
        const parsed = readListState(window.location.hash, nextRoute.name);
        const filtersChanged = !sameFilters(filtersRef.current, parsed.filters);
        setFilters(parsed.filters);
        if (filtersChanged) {
          const setOtherView = nextRoute.name === "history" ? setWorkflowView : setHistoryView;
          setOtherView((current) => ({ ...current, cursors: [null], page: 0 }));
        }
        const setView = nextRoute.name === "history" ? setHistoryView : setWorkflowView;
        setView((current) => {
          const queryChanged = current.query !== parsed.query;
          const changed = queryChanged || current.state !== parsed.state || filtersChanged;
          if (!changed && current.draft === parsed.query && !current.draftComposing) return current;
          return {
            ...current,
            query: parsed.query,
            draft: parsed.query,
            draftComposing: false,
            state: parsed.state,
            resetVersion: current.resetVersion + (queryChanged ? 1 : 0),
            filterResetVersion: current.filterResetVersion + (filtersChanged ? 1 : 0),
            ...(changed ? { cursors: [null], page: 0 } : {}),
          };
        });
      }
      if (nextRoute.name === "dag" && nextRoute.tab === "history") {
        const parsed = readListState(window.location.hash, "history");
        setDagHistory((current) => {
          if (current.dagId !== nextRoute.dagId) {
            return { dagId: nextRoute.dagId, view: makeView(parsed) };
          }
          const view = current.view;
          const changed = view.query !== parsed.query || view.state !== parsed.state;
          if (!changed && view.draft === parsed.query && !view.draftComposing) return current;
          return {
            ...current,
            view: {
              ...view, query: parsed.query, draft: parsed.query, draftComposing: false,
              state: parsed.state, resetVersion: view.resetVersion + (view.query !== parsed.query ? 1 : 0),
              ...(changed ? { cursors: [null], page: 0 } : {}),
            },
          };
        });
      }
      setRoute(nextRoute);
    };
    window.addEventListener("hashchange", update);
    return () => window.removeEventListener("hashchange", update);
  }, []);
  useEffect(() => {
    if (route.name !== "dags" && route.name !== "history") return;
    const actual = currentRoute();
    if (actual.name !== route.name || (route.name === "history" && actual.dagId !== route.dagId)) return;
    const view = route.name === "history" ? historyView : workflowView;
    const href = listHref(route.name, filters, view, route.dagId);
    if (window.location.hash !== href) window.history.replaceState(null, "", href);
  }, [
    route, filters,
    workflowView.query, workflowView.draft, workflowView.draftComposing, workflowView.state,
    historyView.query, historyView.draft, historyView.draftComposing, historyView.state,
  ]);
  useEffect(() => {
    if (route.name !== "dag" || route.tab !== "history" || dagHistory.dagId !== route.dagId) return;
    const actual = currentRoute();
    if (actual.name !== "dag" || actual.dagId !== route.dagId || actual.tab !== "history") return;
    const href = workflowHref(route.dagId, "history", dagHistory.view);
    if (window.location.hash !== href) window.history.replaceState(null, "", href);
  }, [route, dagHistory]);
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
        state: "all", filterResetVersion: current.filterResetVersion + 1,
      } : {}),
      cursors: [null], page: 0,
    }));
    setHistoryView((current) => ({
      ...current,
      ...(page === "history" ? {
        state: "", filterResetVersion: current.filterResetVersion + 1,
      } : {}),
      cursors: [null], page: 0,
    }));
    if (page === "history" && route.name === "history" && route.dagId) {
      const href = listHref("history", emptyFilters(), { ...historyView, state: "" });
      window.history.replaceState(null, "", href);
      setRoute(currentRoute());
    }
  };
  const dagHistoryView = dagHistory.dagId === route.dagId
    ? dagHistory.view : makeView(readListState("", "history"));
  const setDagHistoryView = (update) => setDagHistory((current) => {
    const view = current.dagId === route.dagId
      ? current.view : makeView(readListState("", "history"));
    return { dagId: route.dagId, view: typeof update === "function" ? update(view) : update };
  });
  const workflowsHref = listHref("dags", filters, workflowView);
  const historyHref = listHref("history", filters, historyView);
  return (
    <>
      <header className="topbar">
        <a href={workflowsHref} className="brand" aria-label="easyflow 首页">
          <span className="brand-mark">
            e<span>·</span>
          </span>
          <strong>easyflow</strong>
        </a>
        <span className="header-divider" />
        <nav className="main-nav" aria-label="主导航">
          <a
            href={workflowsHref}
            aria-current={(["dags", "dag"].includes(route.name) || route.fromWorkflow) ? "page" : undefined}
          >
            工作流
          </a>
          <a
            href={historyHref}
            aria-current={route.name.startsWith("history") && !route.fromWorkflow ? "page" : undefined}
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
          tab={route.tab}
          historyView={dagHistoryView}
          setHistoryView={setDagHistoryView}
          backHref={workflowsHref}
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
          allRunsHref={historyHref}
        />
      ) : route.name === "history-run" ? (
        <HistoryRun
          key={JSON.stringify([route.dagId, route.runId])}
          dagId={route.dagId}
          runId={route.runId}
          backHref={route.fromWorkflow
            ? workflowHref(route.dagId, "history", { query: route.backQuery, state: route.backState })
            : listHref("history", filters, historyView, route.backDagId)}
          backLabel={route.fromWorkflow ? "返回工作流运行历史" : undefined}
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
