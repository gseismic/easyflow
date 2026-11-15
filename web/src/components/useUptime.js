import { usePolling } from "../api";
import { uptimeUrl } from "../uptime";

export default function useUptime(dagIds, day, period = "day", hours = 24) {
  return usePolling(uptimeUrl(dagIds, day, period, hours), 5000);
}
