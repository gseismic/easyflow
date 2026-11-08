import { useState } from "react";
import { post } from "../api";
import Modal from "./Modal";

export default function ClearDialog({ runBase, taskId, onClose, onDone }) {
  const [scope, setScope] = useState("task");
  const [preview, setPreview] = useState(null);
  const [busy, setBusy] = useState(false);
  const [error, setError] = useState("");
  const action = async (fn) => {
    setBusy(true);
    setError("");
    try {
      await fn();
    } catch (reason) {
      setError(reason.message);
    } finally {
      setBusy(false);
    }
  };
  const inspect = () =>
    action(async () => {
      setPreview(
        await post(`${runBase}/clear-preview`, {
          task_ids: scope === "task" ? [taskId] : null,
        }),
      );
    });
  const clear = () =>
    action(async () => {
      onDone(await post(`${runBase}/clear`, { task_ids: preview.to_clear }));
    });
  return (
    <Modal title="预览重跑" onClose={onClose}>
      <p>清理后，选中的任务将再次执行，可能重复产生外部影响。</p>
      <label className="field">
        重跑范围
        <select
          value={scope}
          onChange={(event) => {
            setScope(event.target.value);
            setPreview(null);
          }}
        >
          <option value="task">仅当前任务：{taskId}</option>
          <option value="all">本次运行中的全部任务</option>
        </select>
      </label>
      {scope === "task" && (
        <p className="hint">已成功的下游任务不会自动重跑。</p>
      )}
      {preview && (
        <div className="preview-box">
          <strong>将清理 {preview.to_clear.length} 个任务</strong>
          <div>{preview.to_clear.join("、") || "无可清理任务"}</div>
          {preview.blocked.map((item) => (
            <div className="danger-text" key={item.task_id}>
              {item.task_id}：{item.reason}
            </div>
          ))}
        </div>
      )}
      {error && (
        <div role="alert" className="form-error">
          {error}
        </div>
      )}
      <div className="dialog-actions">
        <button className="button subtle" onClick={onClose}>
          取消
        </button>
        {!preview ? (
          <button className="button primary" disabled={busy} onClick={inspect}>
            查看影响
          </button>
        ) : (
          <button
            className="button primary"
            disabled={
              busy || !preview.to_clear.length || !!preview.blocked.length
            }
            onClick={clear}
          >
            清理并等待重跑
          </button>
        )}
      </div>
    </Modal>
  );
}
