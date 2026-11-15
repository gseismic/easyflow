import { useCallback, useEffect, useLayoutEffect, useState } from "react";
import { createAppearanceApi } from "./appearance";

const appearance = createAppearanceApi();
function browserStorage() {
  try { return window.localStorage; } catch { return null; }
}

export default function useAppearance() {
  const [settings, setSettings] = useState(() => appearance.read(browserStorage()));
  const [effectiveTheme, setEffectiveTheme] = useState(() => appearance.resolve(settings));
  const [storageWarning, setStorageWarning] = useState("");
  useLayoutEffect(() => {
    let timer;
    const update = () => {
      window.clearTimeout(timer);
      const now = new Date();
      const theme = appearance.apply(document, settings, now);
      setEffectiveTheme(theme);
      if (settings.mode === "auto") timer = window.setTimeout(update, appearance.nextDelay(now));
    };
    const resume = () => { if (!document.hidden) update(); };
    update();
    window.addEventListener("focus", resume);
    window.addEventListener("pageshow", resume);
    document.addEventListener("visibilitychange", resume);
    return () => {
      window.clearTimeout(timer);
      window.removeEventListener("focus", resume);
      window.removeEventListener("pageshow", resume);
      document.removeEventListener("visibilitychange", resume);
    };
  }, [settings]);
  useEffect(() => {
    const sync = (event) => {
      if (event.key !== appearance.key && event.key !== null) return;
      setSettings(appearance.read(browserStorage()));
      setStorageWarning("");
    };
    window.addEventListener("storage", sync);
    return () => window.removeEventListener("storage", sync);
  }, []);
  const save = useCallback((value) => {
    const result = appearance.save(browserStorage(), value);
    if (result.ok) {
      setSettings(result.settings);
      setStorageWarning(result.persisted ? "" : "外观已在当前页面应用，但浏览器不允许保存；刷新后可能丢失此次更改。");
    }
    return result;
  }, []);
  return { settings, effectiveTheme, save, storageWarning };
}
