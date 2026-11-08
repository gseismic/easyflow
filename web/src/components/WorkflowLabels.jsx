export default function WorkflowLabels({ item, filters, onChange }) {
  const kind = item.project_kind || "unknown";
  const label = kind === "project"
    ? item.project
    : kind === "unassigned"
      ? "未分组"
      : kind === "conflict"
        ? "项目异常"
        : "项目未知";
  return (
    <div className="workflow-labels">
      <button
        type="button"
        className={`project-label ${kind}`}
        title={item.metadata_issue || `筛选项目：${label}`}
        aria-label={`筛选项目：${label}`}
        aria-pressed={filters.projectKind === kind && (kind !== "project" || filters.project === item.project)}
        onClick={() => onChange({ ...filters, projectKind: kind, project: item.project || "" })}
      >
        {item.metadata_issue ? "⚠ " : ""}{label}
      </button>
      {(item.tags || []).map((tag) => (
        <button
          type="button"
          className="tag-label"
          key={tag}
          title={`筛选标签：${tag}`}
          aria-label={`筛选标签：${tag}`}
          aria-pressed={filters.tags.includes(tag)}
          onClick={() => onChange({
            ...filters,
            tags: filters.tags.includes(tag)
              ? filters.tags.filter((item) => item !== tag)
              : [...filters.tags, tag],
          })}
        >
          {tag}
        </button>
      ))}
      {item.metadata_issue && <span className="metadata-issue">{item.metadata_issue}</span>}
    </div>
  );
}
