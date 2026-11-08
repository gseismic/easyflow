import Status from "./Status";

export default function Graph({ tasks, states, selected, onSelect }) {
  if (!tasks.length)
    return <div className="empty compact">这个 DAG 还没有任务。</div>;
  const depth = new Map();
  const rows = new Map();
  const points = new Map();
  tasks.forEach((task) => {
    const level = Math.max(
      0,
      ...task.upstream.map((id) => (depth.get(id) ?? -1) + 1),
    );
    depth.set(task.task_id, level);
    const row = rows.get(level) || 0;
    rows.set(level, row + 1);
    points.set(task.task_id, { x: 24 + level * 256, y: 24 + row * 96 });
  });
  const width = Math.max(400, ...[...points.values()].map((p) => p.x + 216));
  const height = Math.max(150, ...[...points.values()].map((p) => p.y + 88));
  return (
    <div className="graph-scroll">
      <div className="graph" style={{ width, height }}>
        <svg
          className="graph-edges"
          width={width}
          height={height}
          aria-hidden="true"
        >
          <defs>
            <marker
              id="arrow"
              markerWidth="8"
              markerHeight="8"
              refX="6"
              refY="3"
              orient="auto"
            >
              <path d="M0,0 L0,6 L6,3 z" fill="#aab9b2" />
            </marker>
          </defs>
          {tasks.flatMap((task) =>
            task.upstream.map((id) => {
              const from = points.get(id);
              const to = points.get(task.task_id);
              if (!from || !to) return null;
              const start = from.x + 200;
              const end = to.x - 6;
              return (
                <path
                  key={`${id}-${task.task_id}`}
                  d={`M${start} ${from.y + 34} C${start + 24} ${from.y + 34}, ${end - 24} ${to.y + 34}, ${end} ${to.y + 34}`}
                  fill="none"
                  stroke="#aab9b2"
                  strokeWidth="1.5"
                  markerEnd="url(#arrow)"
                />
              );
            }),
          )}
        </svg>
        {tasks.map((task) => {
          const point = points.get(task.task_id);
          return (
            <button
              key={task.task_id}
              onClick={() => onSelect(task.task_id)}
              aria-pressed={selected === task.task_id}
              className={`graph-node ${selected === task.task_id ? "selected" : ""}`}
              style={{ left: point.x, top: point.y }}
            >
              <span className="node-title" title={task.task_id}>
                {task.task_id}
              </span>
              <Status value={states?.[task.task_id]} />
            </button>
          );
        })}
      </div>
    </div>
  );
}
