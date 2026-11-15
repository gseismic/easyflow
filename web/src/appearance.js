// 此工厂同时用于首屏初始化和 React；保持自包含，避免两套时间判断。
export function createAppearanceApi() {
  const key = "easyflow:appearance:v1";
  function defaults() { return { mode: "light", dayStart: "07:00", nightStart: "19:00" }; }
  function minutes(value) {
    return typeof value === "string" && /^([01]\d|2[0-3]):[0-5]\d$/.test(value)
      ? Number(value.slice(0, 2)) * 60 + Number(value.slice(3)) : null;
  }
  function validate(value) {
    if (!value || !["light", "dark", "auto"].includes(value.mode)) return "请选择白天、黑夜或自动主题。";
    if (minutes(value.dayStart) === null || minutes(value.nightStart) === null) return "请填写有效的白天和黑夜开始时间。";
    return value.dayStart === value.nightStart ? "白天和黑夜的开始时间不能相同。" : "";
  }
  function pick(value) { return { mode: value.mode, dayStart: value.dayStart, nightStart: value.nightStart }; }
  function read(storage) {
    try {
      const value = JSON.parse(storage?.getItem(key) || "null");
      return value?.version === 1 && !validate(value) ? pick(value) : defaults();
    } catch { return defaults(); }
  }
  function save(storage, value) {
    const error = validate(value);
    if (error) return { ok: false, error };
    const settings = pick(value);
    try {
      if (!storage) throw new Error("storage unavailable");
      storage.setItem(key, JSON.stringify({ version: 1, ...settings }));
      return { ok: true, persisted: true, settings };
    } catch { return { ok: true, persisted: false, settings }; }
  }
  function resolve(value, now = new Date()) {
    const settings = validate(value) ? defaults() : value;
    if (settings.mode !== "auto") return settings.mode;
    const current = now.getHours() * 60 + now.getMinutes();
    if (!Number.isFinite(current)) return "light";
    const day = minutes(settings.dayStart);
    const night = minutes(settings.nightStart);
    const daytime = day < night ? current >= day && current < night : current >= day || current < night;
    return daytime ? "light" : "dark";
  }
  function nextDelay(now = new Date()) {
    const elapsed = now.getSeconds() * 1000 + now.getMilliseconds();
    return Number.isFinite(elapsed) ? Math.max(1, 60000 - elapsed) : 60000;
  }
  function apply(document, value, now = new Date()) {
    const theme = resolve(value, now);
    document.documentElement.dataset.theme = theme;
    document.querySelector('meta[name="theme-color"]')?.setAttribute("content", theme === "dark" ? "#101913" : "#f5f7f4");
    return theme;
  }
  return { key, defaults, validate, read, save, resolve, nextDelay, apply };
}
