import { useState } from "react";
import { post } from "../api";
import Modal from "./Modal";
import { formatTime } from "./Status";

export default function BackfillDialog({ base, onClose }) {
  const [dates, setDates] = useState({
    start_date: "",
    end_date: "",
    reprocessing_behavior: "none",
  });
  const [preview, setPreview] = useState(null);
  const [busy, setBusy] = useState(false);
  const [error, setError] = useState("");
  const change = (name, value) => {
    setDates({ ...dates, [name]: value });
    setPreview(null);
  };
  const submit = async (event) => {
    event.preventDefault();
    setBusy(true);
    setError("");
    try {
      setPreview(await post(`${base}/backfill-preview`, dates));
    } catch (reason) {
      setError(reason.message);
    } finally {
      setBusy(false);
    }
  };
  return (
    <Modal title="回补日期预览" onClose={onClose}>
      <form onSubmit={submit}>
        <p>检查指定日期范围；实际创建时会重新检查已有运行状态。</p>
        <div className="form-row">
          <label className="field">
            开始日期
            <input
              type="date"
              required
              value={dates.start_date}
              onChange={(event) => change("start_date", event.target.value)}
            />
          </label>
          <label className="field">
            结束日期
            <input
              type="date"
              required
              value={dates.end_date}
              onChange={(event) => change("end_date", event.target.value)}
            />
          </label>
        </div>
        <label className="field">
          已有运行时
          <select
            value={dates.reprocessing_behavior}
            onChange={(event) =>
              change("reprocessing_behavior", event.target.value)
            }
          >
            <option value="none">仅缺失日期</option>
            <option value="failed">缺失或失败</option>
            <option value="completed">缺失或已结束</option>
          </select>
        </label>
        {preview && (
          <div className="preview-box">
            <strong>
              共 {preview.length} 个日期，当前可创建{" "}
              {preview.filter((item) => item.eligible).length} 个
            </strong>
            <div className="preview-dates">
              {preview.slice(0, 30).map((item) => (
                <div key={item.logical_date}>
                  {formatTime(item.logical_date)} ·{" "}
                  {item.eligible ? "可创建" : "跳过"}
                  {item.latest_state ? `（${item.latest_state}）` : ""}
                </div>
              ))}
              {preview.length > 30 && <div>仅显示前 30 项</div>}
            </div>
            <p className="hint">预览不会创建运行；可使用 CLI 执行回补。</p>
          </div>
        )}
        {error && (
          <div role="alert" className="form-error">
            {error}
          </div>
        )}
        <div className="dialog-actions">
          <button type="button" className="button subtle" onClick={onClose}>
            关闭
          </button>
          <button className="button primary" disabled={busy}>
            预览日期
          </button>
        </div>
      </form>
    </Modal>
  );
}
