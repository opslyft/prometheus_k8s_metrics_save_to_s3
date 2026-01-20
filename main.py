#!/usr/bin/env python3
from datetime import datetime, timedelta, timezone
from boto3.s3.transfer import TransferConfig
import io
import boto3
import requests
import logging
import os
import sys
import json
import time


# =========================
# LOGGER
# =========================
def setup_logger(name: str = "metrics_scraper") -> logging.Logger:
    logger = logging.getLogger(name)
    logger.setLevel(logging.INFO)

    if logger.handlers:
        return logger  # avoid duplicate logs in ECS/Lambda

    handler = logging.StreamHandler(sys.stdout)
    formatter = logging.Formatter(
        fmt="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
        datefmt="%Y-%m-%dT%H:%M:%SZ",
    )
    handler.setFormatter(formatter)
    logger.addHandler(handler)
    logger.propagate = False
    return logger


logger = setup_logger()


# =========================
# STATIC CONFIG (CODE ONLY)
# =========================
AWS_REGION = "us-east-1"
S3_BUCKET = os.getenv("S3_BUCKET")  #  Read from environment because bucket names are at a global level hence a unique prefix is must 

if not S3_BUCKET:
    raise RuntimeError("S3_BUCKET env var is required")

BASE_S3_PATH = "k8s_data"
HOURS_TO_BACKFILL = 8

STEP_SECONDS = 3600
TIMEOUT_SECONDS = 30

MAX_RETRIES = 3
RETRY_BACKOFF_SECONDS = 2


METRICS = [
    "kube_node_info",
    "kube_pod_info",
    "container_memory_working_set_bytes",
    "container_cpu_usage_seconds_total",
    "cluster:namespace:pod_cpu:active:kube_pod_container_resource_requests",
    "cluster:namespace:pod_cpu:active:kube_pod_container_resource_limits",
    "cluster:namespace:pod_memory:active:kube_pod_container_resource_requests",
    "cluster:namespace:pod_memory:active:kube_pod_container_resource_limits",
    "kube_pod_container_resource_requests",
    "kube_pod_container_resource_limits",
]

S3_TRANSFER_CONFIG = TransferConfig(
    multipart_threshold=5 * 1024 * 1024,  # 5 MB
    multipart_chunksize=5 * 1024 * 1024,  # 5 MB
    max_concurrency=4,
    use_threads=True,
)


# =========================
# PROM ENDPOINTS (ENV ONLY)
# =========================
def load_prom_endpoints() -> dict:
    raw = os.getenv("PROM_ENDPOINTS")
    if not raw:
        raise RuntimeError("PROM_ENDPOINTS env var is required")

    try:
        endpoints = json.loads(raw)
        if not isinstance(endpoints, dict) or not endpoints:
            raise ValueError
        return endpoints
    except Exception:
        raise RuntimeError("PROM_ENDPOINTS must be a valid JSON object")


PROM_ENDPOINTS = load_prom_endpoints()


# =========================
# HELPERS TO PRINT FIRST 3000 CHAR OF THE RESPONSE IN CASE OF NON 2000
# =========================
def response_snippet(resp, limit=3000) -> str:
    try:
        return resp.text[:limit].replace("\n", " ")
    except Exception:
        return "<unable to read response body>"


# =========================
# SCRAPER
# =========================
def scrape_and_upload_for_hour(offset_hours: int):
    now = datetime.now(timezone.utc)

    hour_boundary = now.replace(minute=0, second=0, microsecond=0)
    end_dt = hour_boundary - timedelta(hours=offset_hours)
    prom_start_dt = end_dt - timedelta(hours=1)
    prom_end_dt = end_dt - timedelta(seconds=1)

    start_epoch = int(prom_start_dt.timestamp())
    end_epoch = int(prom_end_dt.timestamp())

    time_path = prom_start_dt.strftime("%Y_%m_%d/%H")
    s3 = boto3.client("s3", region_name=AWS_REGION)

    for alias, base_url in PROM_ENDPOINTS.items():
        for metric in METRICS:
            params = {
                "query": metric,
                "start": start_epoch,
                "end": end_epoch,
                "step": STEP_SECONDS,
            }

            logger.info(f"Making api call with params as {str(params)}")

            for attempt in range(1, MAX_RETRIES + 1):
                try:
                    r = requests.get(
                        base_url,
                        params=params,
                        timeout=TIMEOUT_SECONDS,
                    )

                    if not (200 <= r.status_code < 300):
                        raise RuntimeError(
                            f"HTTP {r.status_code} | response={response_snippet(r)}"
                        )

                    s3_key = f"{BASE_S3_PATH}/{alias}/{time_path}/{metric}.json"

                    logger.info(f"Generate s3_key as {s3_key} extra = {alias}")

                    file_obj = io.BytesIO(r.content)

                    s3.upload_fileobj(
                        Fileobj=file_obj,
                        Bucket=S3_BUCKET,
                        Key=s3_key,
                        ExtraArgs={"ContentType": "application/json"},
                        Config=S3_TRANSFER_CONFIG,
                    )

                    logger.info(
                        f"[OK] offset={offset_hours} "
                        f"attempt={attempt} "
                        f"s3://{S3_BUCKET}/{s3_key} "
                        f"{prom_start_dt.isoformat()} → {end_dt.isoformat()} extra = {alias}"
                    )
                    break

                except Exception as e:
                    if attempt == MAX_RETRIES:
                        logger.error(
                            f"[FAIL] offset={offset_hours} "
                            f"alias={alias} metric={metric} "
                            f"attempts={MAX_RETRIES} "
                            f"{prom_start_dt.isoformat()} → {end_dt.isoformat()} | ERROR: {str(e)} extra = {alias}"
                        )
                    else:
                        sleep_for = RETRY_BACKOFF_SECONDS * (2 ** (attempt - 1))

                        logger.warning(
                            f"[RETRY] offset={offset_hours} "
                            f"alias={alias} metric={metric} "
                            f"attempt={attempt}/{MAX_RETRIES} "
                            f"sleep={sleep_for}s | ERROR: {str(e)} extra = {alias}"
                        )
                        time.sleep(sleep_for)


# =========================
# MAIN
# =========================
if __name__ == "__main__":
    logger.info(
        f"Starting scrape | hours={HOURS_TO_BACKFILL} "
        f"endpoints={list(PROM_ENDPOINTS.keys())}"
    )

    for offset in range(HOURS_TO_BACKFILL):
        scrape_and_upload_for_hour(offset)

    logger.info("Scrape job completed")
