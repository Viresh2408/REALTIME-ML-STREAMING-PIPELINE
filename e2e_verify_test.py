#!/usr/bin/env python3
"""
E2E Produce Test Script
======================
Authenticates, produces a synthetic event with score=0.97 to:
  POST /api/v1/kafka/produce-test

Then polls GET /api/v1/alerts to verify that:
  - The alert is generated with CRITICAL severity.
  - It appears in the active alerts list within 10 seconds.
"""

from __future__ import annotations

import sys
import time

import httpx

BASE_URL = "http://localhost:8000"
STEP_OK = "[OK]"
STEP_FAIL = "[FAIL]"
STEP_INFO = "[INFO]"


def ok(msg: str) -> None:
    print(f"  {STEP_OK}  {msg}")


def fail(msg: str) -> None:
    print(f"  {STEP_FAIL}  {msg}")
    sys.exit(1)


def info(msg: str) -> None:
    print(f"  {STEP_INFO}  {msg}")


def main() -> None:
    print("\n=== Step 1: Authenticating as viewer ===")
    auth_resp = httpx.post(
        f"{BASE_URL}/api/v1/auth/token",
        data={"username": "viewer@example.com", "password": "viewer123"},
        headers={"Content-Type": "application/x-www-form-urlencoded"},
        timeout=10,
    )
    if auth_resp.status_code != 200:
        fail(f"Authentication failed: {auth_resp.status_code} - {auth_resp.text}")

    token = auth_resp.json()["access_token"]
    headers = {"Authorization": f"Bearer {token}"}
    ok("Successfully authenticated (role=viewer)")

    print("\n=== Step 2: Injecting synthetic scored event (score=0.97) ===")
    # Custom source so we can uniquely filter it
    source_id = f"e2e-verify-{int(time.time())}"
    produce_resp = httpx.post(
        f"{BASE_URL}/api/v1/kafka/produce-test",
        json={"score": 0.97, "source_id": source_id},
        headers=headers,
        timeout=10,
    )

    if produce_resp.status_code != 201:
        fail(f"Produce test failed: {produce_resp.status_code} - {produce_resp.text}")

    resp_data = produce_resp.json()
    event_id = resp_data["event_id"]
    ok(f"Synthetic event produced: event_id={event_id}, source_id={source_id}")

    print("\n=== Step 3: Polling GET /api/v1/alerts for up to 10 seconds ===")
    start_time = time.time()
    deadline = start_time + 10
    attempts = 0
    found_alert = None

    while time.time() < deadline:
        attempts += 1
        try:
            alerts_resp = httpx.get(
                f"{BASE_URL}/api/v1/alerts",
                headers=headers,
                params={"severity": "CRITICAL", "limit": 100},
                timeout=5,
            )
            if alerts_resp.status_code == 200:
                alerts = alerts_resp.json()
                for alert in alerts:
                    if alert.get("source_id") == source_id:
                        found_alert = alert
                        break
                if found_alert:
                    break
                info(
                    f"  Attempt {attempts}: not visible yet "
                    f"(found {len(alerts)} other critical alerts)"
                )
            else:
                info(f"  Attempt {attempts}: HTTP {alerts_resp.status_code}")
        except Exception as e:
            info(f"  Attempt {attempts}: Error polling: {e}")

        time.sleep(0.5)

    duration = time.time() - start_time
    print("\n=== Step 4: Verification Result ===")
    if found_alert:
        ok(
            f"PASS - Alert successfully verified in {duration:.2f} seconds "
            f"after {attempts} poll(s)!"
        )
        print(f"    Alert ID : {found_alert['alert_id']}")
        print(f"    Source ID: {found_alert['source_id']}")
        print(f"    Severity : {found_alert['severity']}")
        print(f"    Status   : {found_alert['status']}")
        print(f"    Score    : {found_alert['score']}")
        sys.exit(0)
    else:
        fail(
            f"FAIL - Alert for source_id={source_id} was not found "
            "in GET /api/v1/alerts within 10 seconds."
        )


if __name__ == "__main__":
    main()
