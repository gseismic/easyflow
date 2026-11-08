import { useEffect } from "react";

export default function usePendingRefresh(resource, pending) {
  useEffect(() => {
    if (!pending || resource.error) return;
    const timer = window.setTimeout(resource.refresh, 1500);
    return () => window.clearTimeout(timer);
  }, [resource.data, resource.error, resource.refresh, pending]);
}
