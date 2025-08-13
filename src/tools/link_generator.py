from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Dict, Iterable, List

import pandas as pd

from src.clients.aws_client import S3Client
from src.utils.logger import get_logger
from src.utils.state import LinkState


@dataclass
class LinkRecord:
    image_name: str
    presigned_url: str
    generated_at: str
    expires_at: str


class LinkGenerator:
    def __init__(self, s3: S3Client, bucket: str, expiry_days: int = 7, max_workers: int = 16):
        self._s3 = s3
        self._bucket = bucket
        self._expiry_days = expiry_days
        self._max_workers = max_workers
        self._log = get_logger("link_generator")

    def _generate_one(self, key: str) -> LinkRecord:
        url = self._s3.generate_presigned_url(self._bucket, key, expires_in_seconds=self._expiry_days * 24 * 3600)
        now = datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")
        # Expires_at is tracked by LinkState when persisting; set empty here and fill on upsert
        return LinkRecord(image_name=key, presigned_url=url, generated_at=now, expires_at="")

    def generate_links(self, keys: List[str], state: LinkState) -> List[LinkRecord]:
        todo = [k for k in keys if state.needs_refresh(k, self._expiry_days)]
        if not todo:
            self._log.info("No links need refresh.")
            return []

        self._log.info(f"Generating {len(todo)} presigned URLs (expiry_days={self._expiry_days}) with max_workers={self._max_workers}")
        results: List[LinkRecord] = []
        with ThreadPoolExecutor(max_workers=self._max_workers) as pool:
            futures = {pool.submit(self._generate_one, key): key for key in todo}
            for fut in as_completed(futures):
                key = futures[fut]
                try:
                    rec = fut.result()
                    # Update state promptly to reduce chance of loss on long runs
                    state.upsert(key, rec.presigned_url, self._expiry_days)
                    # Fill expires_at back from state entry for output
                    entry = state.links[key]
                    rec.generated_at = entry.generated_at
                    rec.expires_at = entry.expires_at
                    results.append(rec)
                except Exception as e:
                    self._log.warning(f"Failed to generate link for {key}: {e}")
        return results

    @staticmethod
    def to_dataframe(records: List[LinkRecord]) -> pd.DataFrame:
        if not records:
            return pd.DataFrame(columns=["image_name", "presigned_url", "generated_at", "expires_at"])
        rows = [
            {
                "image_name": r.image_name,
                "presigned_url": r.presigned_url,
                "generated_at": r.generated_at,
                "expires_at": r.expires_at,
            }
            for r in records
        ]
        return pd.DataFrame(rows)


