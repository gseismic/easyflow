import { useEffect, useId, useRef, useState } from "react";

export default function HorizontalScroll({ className, label, hint, children }) {
  const container = useRef(null);
  const hintId = useId();
  const [overflow, setOverflow] = useState(false);
  useEffect(() => {
    const node = container.current;
    const measure = () => setOverflow(node.scrollWidth > node.clientWidth + 1);
    const observer = new ResizeObserver(measure);
    observer.observe(node);
    if (node.firstElementChild) observer.observe(node.firstElementChild);
    measure();
    return () => observer.disconnect();
  }, []);
  return <>
    <div ref={container} className={className} role="group" aria-label={label}
      aria-describedby={overflow ? hintId : undefined} tabIndex={overflow ? 0 : undefined}>
      {children}
    </div>
    {overflow && <p className="scroll-hint" id={hintId}>{hint}</p>}
  </>;
}
