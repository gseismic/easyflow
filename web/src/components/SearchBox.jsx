import { useEffect, useRef, useState } from "react";

export default function SearchBox({
  query,
  value,
  onDraftChange,
  onSearch,
  resetVersion,
  label,
  describedBy,
  placeholder = "搜索工作流、项目或标签",
}) {
  const [composing, setComposing] = useState(false);
  const inputRef = useRef(null);
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
      draft.onDraftChange(draft.query, false);
      return;
    }
    const next = draft.value.trim();
    if (next !== draft.query) draft.onSearch(next);
  }, []);

  return (
    <div className="search-box search-live">
      <span aria-hidden="true">⌕</span>
      <input
        ref={inputRef}
        aria-label={label}
        aria-describedby={describedBy}
        placeholder={placeholder}
        maxLength={1000}
        value={value}
        onChange={(event) => {
          current.current.value = event.target.value;
          onDraftChange(event.target.value, current.current.composing);
        }}
        onCompositionStart={(event) => {
          current.current.composing = true;
          setComposing(true);
          onDraftChange(event.currentTarget.value, true);
        }}
        onCompositionEnd={(event) => {
          current.current.value = event.currentTarget.value;
          current.current.composing = false;
          onDraftChange(event.currentTarget.value, false);
          setComposing(false);
        }}
        onKeyDown={(event) => {
          if (event.key === "Enter" && !current.current.composing && !event.nativeEvent.isComposing) {
            event.preventDefault();
            const next = event.currentTarget.value.trim();
            if (next !== current.current.query) current.current.onSearch(next);
          }
        }}
      />
      <button
        type="button"
        className={value ? "search-clear" : "search-clear is-hidden"}
        aria-label="清空搜索"
        disabled={!value}
        onClick={() => {
          current.current.value = "";
          current.current.composing = false;
          onDraftChange("", false);
          setComposing(false);
          if (current.current.query) current.current.onSearch("");
          inputRef.current?.focus();
        }}
      >
        ×
      </button>
    </div>
  );
}
