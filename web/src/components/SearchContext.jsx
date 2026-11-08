function words(query) {
  return query.trim().toLocaleLowerCase().split(/\s+/).filter(Boolean);
}

function patternFor(terms) {
  return terms
    .map((term) => term.replace(/[.*+?^$(){}|[\]\\]/g, "\\$&"))
    .sort((left, right) => right.length - left.length)
    .join("|");
}

function excerpt(text, pattern) {
  const first = pattern ? (new RegExp(pattern, "iu").exec(text)?.index ?? 0) : 0;
  const start = Math.max(0, first - 40);
  const end = Math.min(text.length, start + 180);
  return (start ? "…" : "") + text.slice(start, end) + (end < text.length ? "…" : "");
}

function highlight(text, pattern) {
  if (!pattern) return text;
  const parts = [];
  let offset = 0;
  for (const match of text.matchAll(new RegExp(pattern, "giu"))) {
    if (match.index > offset) parts.push(text.slice(offset, match.index));
    parts.push(<mark key={match.index}>{match[0]}</mark>);
    offset = match.index + match[0].length;
  }
  parts.push(text.slice(offset));
  return parts;
}

export default function SearchContext({ item, query, history = false }) {
  const pattern = patternFor(words(query));
  const description = item.description || "";
  const matchedTasks = item.matched_task_ids || [];
  if (!description && !matchedTasks.length) return null;
  return (
    <div className="search-context">
      {description && (
        <small className="workflow-description" title={description}>
          {history && "当前描述："}{highlight(excerpt(description, pattern), pattern)}
        </small>
      )}
      {matchedTasks.length > 0 && (
        <small className="matched-tasks">匹配任务：{matchedTasks.map((id, index) => (
          <span key={id}>{index > 0 && "、"}<code>{highlight(id, pattern)}</code></span>
        ))}{matchedTasks.length === 3 && "（最多显示 3 项）"}</small>
      )}
    </div>
  );
}
