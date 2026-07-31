"""
watchdog.py
============

Answers one question continuously: "is at least one of the two risk-scoring
engines actually up right now?" -- and exposes that as a Prometheus metric
so it can be graphed and alerted on, rather than something an operator has
to notice by absence of alerts.

It does NOT perform failover itself -- there is nothing to fail *over* to,
because both engines already run all the time (see secondary_risk_engine.py's
module docstring for why active-active was chosen over active-passive). This
process is purely observability: it turns "is the primary Flink job's
RiskScoringJob actually RUNNING" and "is the secondary Python engine's
consumer loop alive" into two Prometheus gauges plus one combined
redundancy-status gauge, polled every WATCHDOG_INTERVAL_SEC seconds.

  fraud_engine_primary_up    {0,1}  -- Flink job status, via JobManager REST API
  fraud_engine_secondary_up  {0,1}  -- Python engine liveness, via its own /metrics
  fraud_engine_redundancy_status
      0 = BOTH ENGINES DOWN   (no fraud scoring happening at all -- page someone)
      1 = SECONDARY ONLY      (primary is down, backstop is covering)
      2 = PRIMARY ONLY        (secondary is down, primary is covering)
      3 = BOTH UP             (normal operating state)
"""

import os
import time
import json
import urllib.request
import urllib.error

from prometheus_client import start_http_server, Gauge

FLINK_JOBMANAGER_URL = os.getenv("FLINK_JOBMANAGER_URL", "http://flink-jobmanager:8081")
SECONDARY_ENGINE_METRICS_URL = os.getenv(
    "SECONDARY_ENGINE_METRICS_URL", "http://fraud-engine-secondary:9500/metrics"
)
WATCHDOG_METRICS_PORT = int(os.getenv("WATCHDOG_METRICS_PORT", "9600"))
WATCHDOG_INTERVAL_SEC = int(os.getenv("WATCHDOG_INTERVAL_SEC", "15"))
HTTP_TIMEOUT_SEC = 5

PRIMARY_UP = Gauge("fraud_engine_primary_up", "1 if the Flink RiskScoringJob has a RUNNING job")
SECONDARY_UP = Gauge("fraud_engine_secondary_up", "1 if the Python secondary engine is reachable and reporting")
REDUNDANCY_STATUS = Gauge(
    "fraud_engine_redundancy_status",
    "0=both down 1=secondary-only 2=primary-only 3=both up",
)


def check_primary() -> bool:
    """Flink's JobManager exposes GET /jobs -> {"jobs": [{"id": ..., "status": "RUNNING"}, ...]}"""
    try:
        with urllib.request.urlopen(f"{FLINK_JOBMANAGER_URL}/jobs", timeout=HTTP_TIMEOUT_SEC) as resp:
            data = json.loads(resp.read().decode("utf-8"))
            jobs = data.get("jobs", [])
            return any(job.get("status") == "RUNNING" for job in jobs)
    except (urllib.error.URLError, TimeoutError, json.JSONDecodeError, OSError):
        return False


def check_secondary() -> bool:
    """The secondary engine exposes secondary_engine_up 1 on its own /metrics endpoint
    (see secondary_risk_engine.py). Reachability + that gauge == 1 counts as up."""
    try:
        with urllib.request.urlopen(SECONDARY_ENGINE_METRICS_URL, timeout=HTTP_TIMEOUT_SEC) as resp:
            body = resp.read().decode("utf-8")
            for line in body.splitlines():
                if line.startswith("secondary_engine_up "):
                    return line.strip().endswith(" 1.0") or line.strip().endswith(" 1")
            return False
    except (urllib.error.URLError, TimeoutError, OSError):
        return False


def main():
    start_http_server(WATCHDOG_METRICS_PORT)
    print(f"🐕 Fraud Engine Watchdog online. Metrics on :{WATCHDOG_METRICS_PORT}/metrics")
    print(f"   Polling primary={FLINK_JOBMANAGER_URL}/jobs every {WATCHDOG_INTERVAL_SEC}s")
    print(f"   Polling secondary={SECONDARY_ENGINE_METRICS_URL} every {WATCHDOG_INTERVAL_SEC}s")

    while True:
        primary = check_primary()
        secondary = check_secondary()

        PRIMARY_UP.set(1 if primary else 0)
        SECONDARY_UP.set(1 if secondary else 0)

        if not primary and not secondary:
            status = 0
            print("🔴 [WATCHDOG] BOTH ENGINES DOWN -- fraud scoring is not happening. Page someone.")
        elif primary and not secondary:
            status = 2
            print("🟡 [WATCHDOG] Secondary engine down -- primary (Flink) is covering alone.")
        elif secondary and not primary:
            status = 1
            print("🟡 [WATCHDOG] Primary engine (Flink) down -- secondary is covering alone.")
        else:
            status = 3

        REDUNDANCY_STATUS.set(status)
        time.sleep(WATCHDOG_INTERVAL_SEC)


if __name__ == "__main__":
    main()