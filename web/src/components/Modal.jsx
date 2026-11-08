import { useEffect, useRef } from "react";

export default function Modal({ title, onClose, children }) {
  const ref = useRef(null);
  useEffect(() => {
    if (ref.current && !ref.current.open) ref.current.showModal();
  }, []);
  return (
    <dialog
      ref={ref}
      className="dialog"
      aria-label={title}
      onCancel={(event) => {
        event.preventDefault();
        onClose();
      }}
    >
      <div className="dialog-head">
        <h2>{title}</h2>
        <button className="icon-button" onClick={onClose} aria-label="关闭">
          ×
        </button>
      </div>
      {children}
    </dialog>
  );
}
