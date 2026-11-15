import { usePolling } from "../api";
import { uptimeUrl } from "../uptime";

export default function useUptime(dagIds, day, period = "day") {
  return usePolling(uptimeUrl(dagIds, day, period), 5000);
}
