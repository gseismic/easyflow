import { useEffect, useState } from "react";
import DagList from "./pages/DagList";
import DagDetail from "./pages/DagDetail";
import { usePolling } from "./api";

function currentDag() {
  const match = window.location.hash.match(/^#\/dags\/(.+)$/);
  try {
    return match ? decodeURIComponent(match[1]) : null;
  } catch {
    return null;
  }
}

export default function App() {
  const [dagId, setDagId] = useState(currentDag);
  const overview = usePolling("/api/dags", 5000);
  useEffect(() => {
    const update = () => setDagId(currentDag());
    window.addEventListener("hashchange", update);
    return () => window.removeEventListener("hashchange", update);
  }, []);

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
        <span
          className={`scheduler-badge ${overview.data?.scheduler_running ? "online" : ""}`}
        >
          <span className="status-dot" />
          {overview.data?.scheduler_running ? "调度器运行中" : "调度器未连接"}
        </span>
      </header>
      {dagId ? (
        <DagDetail
          key={dagId}
          dagId={dagId}
          refreshOverview={overview.refresh}
        />
      ) : (
        <DagList overview={overview} />
      )}
    </>
  );
}
