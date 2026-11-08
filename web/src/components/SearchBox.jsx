import { useEffect, useRef, useState } from "react";

export default function SearchBox({
  query,
  value,
  onDraftChange,
  onSearch,
  resetVersion,
  label,
  placeholder = "搜索工作流、项目或标签",
}) {
  const [composing, setComposing] = useState(false);
  const current = useRef({ query, value, onDraftChange, onSearch, composing: false });
  current.current = { query, value, onDraftChange, onSearch, composing: current.current.composing };

  useEffect(() => {
    if (composing) return;
    const timer = window.setTimeout(() => {
      const next = value.trim();
      if (next !== current.current.query) current.current.onSearch(next);
    }, 300);
    return () => window.clearTimeout(timer);
  }, [value, composing]);

  useEffect(() => {
    current.current.composing = false;
    setComposing(false);
  }, [resetVersion]);

  useEffect(() => () => {
    const draft = current.current;
    if (draft.composing) {
      if (draft.value !== draft.query) draft.onDraftChange(draft.query);
      return;
    }
    const next = draft.value.trim();
    if (next !== draft.query) draft.onSearch(next);
  }, []);

  return (
    <div className="search-box search-live">
      <span aria-hidden="true">⌕</span>
      <input
        aria-label={label}
        placeholder={placeholder}
        maxLength={1000}
        value={value}
        onChange={(event) => {
          current.current.value = event.target.value;
          onDraftChange(event.target.value);
        }}
        onCompositionStart={() => {
          current.current.composing = true;
          setComposing(true);
        }}
        onCompositionEnd={(event) => {
          current.current.value = event.currentTarget.value;
          current.current.composing = false;
          onDraftChange(event.currentTarget.value);
          setComposing(false);
        }}
      />
      {value && (
        <button
          type="button"
          className="search-clear"
          aria-label="清空搜索"
          onClick={() => {
            current.current.value = "";
            current.current.composing = false;
            onDraftChange("");
            setComposing(false);
            if (current.current.query) current.current.onSearch("");
          }}
        >
          ×
        </button>
      )}
    </div>
  );
}
