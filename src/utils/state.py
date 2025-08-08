import json
from dataclasses import dataclass, asdict
from pathlib import Path
from typing import Dict, Optional


@dataclass
class IncrementalState:
    """
    Tracks seen S3 objects by key + etag so we can do incremental loads efficiently.
    """
    seen: Dict[str, str]

    @staticmethod
    def load(path: Path) -> "IncrementalState":
        if not path.exists():
            return IncrementalState(seen={})
        with path.open("r", encoding="utf-8") as f:
            data = json.load(f)
        return IncrementalState(seen=data.get("seen", {}))

    def save(self, path: Path) -> None:
        path.parent.mkdir(parents=True, exist_ok=True)
        with path.open("w", encoding="utf-8") as f:
            json.dump(asdict(self), f, indent=2)

    def needs_processing(self, key: str, etag: str) -> bool:
        return self.seen.get(key) != etag

    def mark_processed(self, key: str, etag: str) -> None:
        self.seen[key] = etag


