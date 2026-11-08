import { useState } from "react";
import { post } from "../api";
import Modal from "./Modal";

export default function TriggerDialog({ base, paused, onClose, onDone }) {
  const [conf, setConf] = useState("{}");
  const [busy, setBusy] = useState(false);
  const [error, setError] = useState("");
  const submit = async (event) => {
    event.preventDefault();
    setBusy(true);
    setError("");
    try {
      let value;
      try {
        value = JSON.parse(conf);
      } catch {
        throw new Error("运行参数必须是有效的 JSON 对象。");
      }
      if (!value || typeof value !== "object" || Array.isArray(value))
        throw new Error("运行参数必须是 JSON 对象。");
      onDone(await post(`${base}/trigger`, { conf: value }));
    } catch (reason) {
      setError(reason.message);
    } finally {
      setBusy(false);
    }
  };
  return (
    <Modal title="触发运行" onClose={onClose}>
      <form onSubmit={submit}>
        <p>创建一次手动运行。任务可能对外部系统产生影响。</p>
        {paused && (
          <div className="notice warning">
            工作流已暂停；运行会在恢复后开始。
          </div>
        )}
        <label className="field">
          运行参数 <span>JSON 对象，可留空对象</span>
          <textarea
            value={conf}
            onChange={(event) => setConf(event.target.value)}
            rows="5"
            spellCheck="false"
          />
        </label>
        {error && (
          <div role="alert" className="form-error">
            {error}
          </div>
        )}
        <div className="dialog-actions">
          <button type="button" className="button subtle" onClick={onClose}>
            取消
          </button>
          <button className="button primary" disabled={busy}>
            创建运行
          </button>
        </div>
      </form>
    </Modal>
  );
}
