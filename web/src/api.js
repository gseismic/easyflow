import { useCallback, useEffect, useState } from "react";

export async function api(path, options = {}) {
  const response = await fetch(path, {
    ...options,
    headers: { "Content-Type": "application/json", ...options.headers },
  });
  const result = await response.json();
  if (!response.ok)
    throw new Error(
      typeof result.detail === "string"
        ? result.detail
        : "请求失败，请检查输入。",
    );
  return result;
}

export const post = (path, body) =>
  api(path, { method: "POST", body: JSON.stringify(body) });
export const part = (value) => encodeURIComponent(value);

export function usePolling(path, interval = 5000) {
  const [result, setResult] = useState({ path: null, data: null, error: "" });
  const [revision, setRevision] = useState(0);
  const refresh = useCallback(() => setRevision((value) => value + 1), []);
  useEffect(() => {
    if (!path) return;
    let active = true;
    const load = async () => {
      try {
        const result = await api(path);
        if (active) setResult({ path, data: result, error: "" });
      } catch (reason) {
        if (active)
          setResult((previous) => ({
            path,
            data: previous.path === path ? previous.data : null,
            error: reason.message,
          }));
      }
    };
    load();
    const timer = window.setInterval(load, interval);
    return () => {
      active = false;
      window.clearInterval(timer);
    };
  }, [path, interval, revision]);
  return {
    data: result.path === path ? result.data : null,
    error: result.path === path ? result.error : "",
    refresh,
  };
}
