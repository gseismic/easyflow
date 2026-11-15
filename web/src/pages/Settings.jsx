import { useEffect, useState } from "react";
import { createAppearanceApi } from "../appearance";

const appearanceApi = createAppearanceApi();
const modes = [["light", "白天", "始终使用浅色"], ["dark", "黑夜", "始终使用深色"], ["auto", "自动", "按设定时间切换"]];

export default function Settings({ appearance }) {
  const { settings, effectiveTheme, save, storageWarning } = appearance;
  const [dayStart, setDayStart] = useState(settings.dayStart);
  const [nightStart, setNightStart] = useState(settings.nightStart);
  const [message, setMessage] = useState("");
  useEffect(() => { setDayStart(settings.dayStart); setNightStart(settings.nightStart); }, [settings.dayStart, settings.nightStart]);
  const automatic = settings.mode === "auto";
  const candidate = { ...settings, dayStart, nightStart };
  const error = appearanceApi.validate(candidate);
  const changed = dayStart !== settings.dayStart || nightStart !== settings.nightStart;
  const changeMode = (mode) => {
    const result = save({ ...settings, mode });
    setMessage(result.ok && result.persisted ? "主题模式已保存。" : "");
  };
  return <main className="page settings-page">
    <div className="page-title-row page-heading-compact">
      <div><h1>设置</h1><p className="subtitle">外观与工作台偏好</p></div>
    </div>
    <section className="card settings-card" aria-labelledby="appearance-title">
      <div className="section-heading"><div><h2 id="appearance-title">外观</h2><p>当前显示：{effectiveTheme === "dark" ? "黑夜" : "白天"}</p></div></div>
      <div className="settings-body">
        <fieldset className="theme-mode-fieldset">
          <legend>主题模式</legend>
          <div className="theme-modes">{modes.map(([mode, label, description]) => <label className="theme-option" key={mode}>
            <span><input type="radio" name="appearance-mode" value={mode} aria-label={label} checked={settings.mode === mode} onChange={() => changeMode(mode)} />{label}</span>
            <small>{description}</small>
          </label>)}</div>
        </fieldset>
        <form className="automatic-theme-form" aria-label="自动主题时间" noValidate onSubmit={(event) => {
          event.preventDefault();
          if (!automatic || error) return;
          const result = save(candidate);
          setMessage(result.ok && result.persisted ? "自动切换时间已保存。" : "");
        }}>
          <fieldset disabled={!automatic}>
            <legend>自动切换时间</legend>
            <div className="theme-time-fields">
              <label>白天开始<input type="time" step="60" required value={dayStart} aria-describedby="appearance-time-help appearance-time-error" aria-invalid={Boolean(automatic && error)} onChange={(event) => { setDayStart(event.target.value); setMessage(""); }} /></label>
              <label>黑夜开始<input type="time" step="60" required value={nightStart} aria-describedby="appearance-time-help appearance-time-error" aria-invalid={Boolean(automatic && error)} onChange={(event) => { setNightStart(event.target.value); setMessage(""); }} /></label>
              <button className="button primary" disabled={!changed || Boolean(error)}>保存自动时间</button>
            </div>
          </fieldset>
          <p className="settings-help" id="appearance-time-help">使用设备本地时间，时间修改保存后生效。白天开始至黑夜开始使用白天主题，其余时间使用黑夜主题，支持跨午夜。</p>
          <p className="form-error" id="appearance-time-error" role={automatic && error ? "alert" : undefined}>{automatic ? error : ""}</p>
        </form>
        <p className="settings-help">外观设置保存在当前浏览器，刷新后保留，并同步到这个浏览器中的其他标签页。</p>
        {storageWarning ? <div className="notice warning settings-storage-warning" role="status"><span>{storageWarning}</span><button className="link-button" onClick={() => {
          const result = save(settings);
          setMessage(result.ok && result.persisted ? "外观设置已保存。" : "");
        }}>重试保存</button></div> : message && <p className="settings-save-message" role="status">{message}</p>}
      </div>
    </section>
    <section className="card settings-card" aria-labelledby="notifications-title">
      <div className="section-heading"><h2 id="notifications-title">告警通知</h2><span className="settings-future-label">规划中</span></div>
      <div className="settings-body"><p className="settings-help">后续可在这里设置告警条件、通知渠道和收件人，例如邮件通知。当前尚未启用，不会发送邮件。</p></div>
    </section>
  </main>;
}
