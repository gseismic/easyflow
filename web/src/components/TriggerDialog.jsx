import { useEffect, useRef, useState } from "react";
import { post } from "../api";
import { triggerDagUrl } from "../dagApi";
import Modal from "./Modal";

export default function TriggerDialog({ dagId, paused, onClose, onDone }) {
  const [conf, setConf] = useState("{}");
  const [busy, setBusy] = useState(false);
  const [error, setError] = useState("");
  const active = useRef(true);
  useEffect(() => {
    active.current = true;
    return () => { active.current = false; };
  }, []);
  const close = () => {
    active.current = false;
    onClose();
  };
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
      const result = await post(triggerDagUrl(dagId), { conf: value });
      if (active.current) onDone(result);
    } catch (reason) {
      if (active.current) setError(reason.message);
    } finally {
      if (active.current) setBusy(false);
    }
  };
  return (
    <Modal title="触发运行" onClose={close}>
      <form onSubmit={submit}>
        <p>创建一次手动运行。任务可能对外部系统产生影响。</p>
        {busy && <p className="hint">关闭窗口不会取消已提交的运行，可在运行记录中查看结果。</p>}
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
          <button type="button" className="button subtle" onClick={close}>
            {busy ? "关闭窗口" : "取消"}
          </button>
          <button className="button primary" disabled={busy}>
            创建运行
          </button>
        </div>
      </form>
    </Modal>
  );
}
