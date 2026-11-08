from __future__ import annotations

import json
import os
import tempfile
from datetime import date, datetime, time, timedelta, timezone
from pathlib import Path

if os.environ.get("EASYFLOW_EXAMPLE_ENGINE") == "airflow":
    from airflow.sdk import DAG, get_current_context, task
else:
    from easyflow.sdk import DAG, get_current_context, task


with DAG(
    "quant_daily_prices",
    schedule="@daily",
    start_date=datetime(2026, 11, 8, 8, tzinfo=timezone.utc),
    catchup=False,
    params={"symbol": "DEMO", "data_root": "./data/prices"},
    max_active_runs=1,
    max_active_tasks=2,
) as dag:

    @task(retries=2)
    def fetch_daily_prices() -> dict[str, object]:
        context = get_current_context()
        symbol = context["params"]["symbol"]
        conf = context["dag_run"].conf or {}
        trade_date = conf.get("trade_date")
        logical_date = context.get("logical_date")
        if trade_date is not None:
            day = date.fromisoformat(str(trade_date))
            interval_start = datetime.combine(day, time.min, tzinfo=timezone.utc)
            interval_end = interval_start + timedelta(days=1)
        elif logical_date is not None:
            interval_end = logical_date
            interval_start = interval_end - timedelta(days=1)
        else:
            raise ValueError("未提供逻辑日期；请在运行参数中指定 trade_date，例如 2026-07-01。")
        ds = interval_start.date().isoformat()
        data_root = Path(context["params"]["data_root"])
        partition = data_root / f"symbol={symbol}" / f"date={ds}"
        final_file = partition / "data.json"
        partition.mkdir(parents=True, exist_ok=True)

        payload = {
            "symbol": symbol,
            "date": ds,
            "data_interval_start": interval_start.isoformat(),
            "data_interval_end": interval_end.isoformat(),
            "logical_date": logical_date.isoformat() if logical_date else None,
            "rows": [{"close": 100.0}],
        }
        temp_file = None
        try:
            with tempfile.NamedTemporaryFile(
                mode="w", encoding="utf-8", dir=partition, prefix="data-", suffix=".tmp", delete=False
            ) as handle:
                temp_file = Path(handle.name)
                json.dump(payload, handle, ensure_ascii=False)
            temp_file.replace(final_file)
        finally:
            if temp_file is not None:
                temp_file.unlink(missing_ok=True)
        return {
            "path": str(final_file),
            "row_count": len(payload["rows"]),
        }

    fetch_daily_prices()
