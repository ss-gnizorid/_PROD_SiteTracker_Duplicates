from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass
from typing import Dict, Iterable, List

import pandas as pd

from src.clients.aws_client import S3Client, S3Object
from src.tools.permutation_generator import PermutationConfig, PermutationGenerator
from src.utils.logger import get_logger


@dataclass
class HashRecord:
    image_name: str
    job_number: str
    job_url: str | None
    hashes: Dict[str, str]

    def to_row(self) -> Dict[str, str]:
        row: Dict[str, str] = {
            "image_name": self.image_name,
            "job_number": self.job_number,
            "job_url": self.job_url or "",
        }
        row.update(self.hashes)
        return row


class HashIndexer:
    def __init__(self, s3: S3Client, generator: PermutationGenerator, max_workers: int = 16):
        self._s3 = s3
        self._generator = generator
        self._max_workers = max_workers
        self._log = get_logger("hash_indexer")

    def _process_one(self, obj: S3Object) -> HashRecord:
        image_bytes = self._s3.stream_bytes(obj.bucket, obj.key)
        hashes = self._generator.hashes_for_image(image_bytes)
        return HashRecord(image_name=obj.key, job_number=obj.job_number, job_url=obj.job_url, hashes=hashes)

    def build_dataframe(self, objects: List[S3Object]) -> pd.DataFrame:
        records: List[HashRecord] = []
        with ThreadPoolExecutor(max_workers=self._max_workers) as pool:
            futures = {pool.submit(self._process_one, obj): obj for obj in objects}
            for fut in as_completed(futures):
                try:
                    rec = fut.result()
                    records.append(rec)
                except Exception as e:
                    obj = futures[fut]
                    self._log.warning(f"Failed to process {obj.key}: {e}")
        rows = [r.to_row() for r in records]
        df = pd.DataFrame(rows)
        # Ensure consistent column ordering: meta first then hashes
        meta_cols = ["image_name", "job_number", "job_url"]
        hash_cols = sorted([c for c in df.columns if c.endswith("_hash")])
        return df[meta_cols + hash_cols]


