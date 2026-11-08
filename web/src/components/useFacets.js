import { useEffect } from "react";
import { usePolling } from "../api";

export default function useFacets(path) {
  const facets = usePolling(path, 15000);
  useEffect(() => {
    if (!facets.data?.pending) return;
    const timer = window.setTimeout(facets.refresh, 1500);
    return () => window.clearTimeout(timer);
  }, [facets.data, facets.refresh]);
  return facets;
}
