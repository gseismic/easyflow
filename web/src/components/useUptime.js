import { usePolling } from "../api";
import { uptimeUrl } from "../uptime";

export default function useUptime(dagIds, day) {
  return usePolling(uptimeUrl(dagIds, day), 5000);
}
