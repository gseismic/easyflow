from __future__ import annotations

import json
from pathlib import Path

from easyflow.sdk import DAG, get_current_context, task


with DAG(
    "quant_daily_prices",
    schedule="@daily",
    params={"symbol": "DEMO", "data_root": "./data/prices"},
    max_active_runs=1,
    max_active_tasks=2,
) as dag:

    @task(retries=2)
    def fetch_daily_prices() -> dict[str, object]:
        context = get_current_context()
        symbol = context["params"]["symbol"]
        ds = context["ds"]
        data_root = Path(context["params"]["data_root"])
        partition = data_root / f"symbol={symbol}" / f"date={ds}"
        temp_file = partition / "data.tmp"
        final_file = partition / "data.json"
        partition.mkdir(parents=True, exist_ok=True)

        payload = {
            "symbol": symbol,
            "date": ds,
            "data_interval_start": context["data_interval_start"].isoformat(),
            "data_interval_end": context["data_interval_end"].isoformat(),
            "rows": [{"close": 100.0}],
        }
        temp_file.write_text(json.dumps(payload, ensure_ascii=False), encoding="utf-8")
        temp_file.replace(final_file)
        return {
            "path": str(final_file),
            "row_count": len(payload["rows"]),
        }

    fetch_daily_prices()
