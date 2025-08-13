import json
from dataclasses import dataclass, asdict
from pathlib import Path
from typing import Dict, Optional
from datetime import datetime, timezone, timedelta


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


@dataclass
class LinkEntry:
    url: str
    generated_at: str  # ISO 8601 UTC
    expires_at: str    # ISO 8601 UTC


@dataclass
class LinkState:
    """
    Tracks presigned URL generation timestamps per S3 key so we can refresh only when needed.
    """
    links: Dict[str, LinkEntry]

    @staticmethod
    def load(path: Path) -> "LinkState":
        if not path.exists():
            return LinkState(links={})
        with path.open("r", encoding="utf-8") as f:
            data = json.load(f)
        links: Dict[str, LinkEntry] = {}
        for key, val in data.get("links", {}).items():
            links[key] = LinkEntry(
                url=val.get("url", ""),
                generated_at=val.get("generated_at", ""),
                expires_at=val.get("expires_at", ""),
            )
        return LinkState(links=links)

    def save(self, path: Path) -> None:
        path.parent.mkdir(parents=True, exist_ok=True)
        as_dict: Dict[str, Dict[str, str]] = {
            key: {
                "url": entry.url,
                "generated_at": entry.generated_at,
                "expires_at": entry.expires_at,
            }
            for key, entry in self.links.items()
        }
        with path.open("w", encoding="utf-8") as f:
            json.dump({"links": as_dict}, f, indent=2)

    def needs_refresh(self, key: str, expiry_days: int) -> bool:
        entry = self.links.get(key)
        if not entry:
            return True
        try:
            gen = datetime.fromisoformat(entry.generated_at.replace("Z", "+00:00"))
        except Exception:
            return True
        now = datetime.now(timezone.utc)
        age_days = (now - gen).total_seconds() / 86400.0
        return age_days >= float(expiry_days)

    def upsert(self, key: str, url: str, expiry_days: int) -> None:
        now = datetime.now(timezone.utc)
        gen_iso = now.isoformat().replace("+00:00", "Z")
        exp = now + timedelta(days=expiry_days)
        exp_iso = exp.isoformat().replace("+00:00", "Z")
        self.links[key] = LinkEntry(url=url, generated_at=gen_iso, expires_at=exp_iso)



