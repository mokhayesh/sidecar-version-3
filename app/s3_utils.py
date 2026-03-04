import boto3
import requests
import io
import csv
import os
import urllib3
from datetime import datetime
from botocore import UNSIGNED
from botocore.config import Config
from app.settings import defaults

# Disable SSL warnings
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

# ╔═════════════════════════════════════════════════════════════════════════╗
# ║                          S3 Utility Functions                          ║
# ╚═════════════════════════════════════════════════════════════════════════╝

def _make_s3_client(anonymous: bool = False):
    """Return a boto3 S3 client, optionally using anonymous access."""
    if anonymous:
        return boto3.client("s3", config=Config(signature_version=UNSIGNED))
    return boto3.Session(
        aws_access_key_id=defaults.get("aws_access_key_id") or None,
        aws_secret_access_key=defaults.get("aws_secret_access_key") or None,
        aws_session_token=defaults.get("aws_session_token") or None,
        region_name=defaults.get("aws_s3_region") or None,
    ).client("s3")

def download_text_from_uri(uri: str) -> str:
    """Download and return the contents of a text file from S3 or HTTP(S) URI."""
    if uri.startswith("s3://"):
        _, rest = uri.split("s3://", 1)
        bucket, key = rest.split("/", 1)
        for anonymous in (False, True):
            try:
                obj = _make_s3_client(anonymous).get_object(Bucket=bucket, Key=key)
                return obj["Body"].read().decode()
            except Exception:
                if anonymous:
                    region = defaults.get("aws_s3_region", "us-east-1")
                    url = f"https://{bucket}.s3.{region}.amazonaws.com/{key}"
                    r = requests.get(url, verify=False, timeout=60)
                    r.raise_for_status()
                    return r.text
                continue
    # Handle HTTP(S) fallback
    r = requests.get(uri, verify=False, timeout=60)
    r.raise_for_status()
    return r.text

def upload_to_s3(process: str, headers, data) -> str:
    """Upload a CSV to the appropriate S3 bucket for the given process."""
    bucket = defaults.get(f"aws_{process.lower()}_bucket", "").strip()
    if not bucket:
        return f"No bucket configured for {process}"

    buf = io.StringIO()
    csv.writer(buf).writerows([headers, *data])
    key = f"{process}_{datetime.now():%Y%m%d_%H%M%S}.csv"

    try:
        _make_s3_client().put_object(Bucket=bucket, Key=key, Body=buf.getvalue())
        return f"Uploaded to s3://{bucket}/{key}"
    except Exception as e:
        return f"S3 upload failed: {e}"
