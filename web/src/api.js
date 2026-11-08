import { useCallback, useEffect, useState } from "react";

let csrfToken = "";

export function setCsrfToken(value) {
  csrfToken = value || "";
}

export async function api(path, options = {}) {
  const response = await fetch(path, {
    ...options,
    headers: {
      "Content-Type": "application/json",
      ...(csrfToken ? { "X-CSRF-Token": csrfToken } : {}),
      ...options.headers,
    },
  });
  const result = await response.json().catch(() => ({}));
  if (!response.ok) {
    if (response.status === 401 && path !== "/api/auth/login") {
      window.dispatchEvent(new Event("easyflow:unauthorized"));
    }
    const error = new Error(
      typeof result.detail === "string"
        ? result.detail
        : "请求失败，请检查输入。",
    );
    error.status = response.status;
    throw error;
  }
  return result;
}

export const post = (path, body) =>
  api(path, { method: "POST", body: JSON.stringify(body) });
export const part = (value) => encodeURIComponent(value);

export function usePolling(path, interval = 5000) {
  const [result, setResult] = useState({
    path: null,
    data: null,
    error: "",
    updatedAt: null,
  });
  const [revision, setRevision] = useState(0);
  const refresh = useCallback(() => setRevision((value) => value + 1), []);
  useEffect(() => {
    if (!path) return;
    let active = true;
    let inFlight = false;
    let controller = null;
    const load = async () => {
      if (inFlight) return;
      inFlight = true;
      controller = new AbortController();
      const timeout = window.setTimeout(() => controller.abort(), 15000);
      try {
        const result = await api(path, { signal: controller.signal });
        if (active)
          setResult({ path, data: result, error: "", updatedAt: Date.now() });
      } catch (reason) {
        if (active)
          setResult((previous) => ({
            path,
            data: previous.path === path ? previous.data : null,
            error:
              reason.name === "AbortError"
                ? "请求超时，请重试。"
                : reason.message,
            updatedAt: previous.path === path ? previous.updatedAt : null,
          }));
      } finally {
        window.clearTimeout(timeout);
        inFlight = false;
      }
    };
    load();
    const timer = window.setInterval(load, interval);
    return () => {
      active = false;
      window.clearInterval(timer);
      controller?.abort();
    };
  }, [path, interval, revision]);
  return {
    data: result.path === path ? result.data : null,
    error: result.path === path ? result.error : "",
    updatedAt: result.path === path ? result.updatedAt : null,
    refresh,
  };
}
