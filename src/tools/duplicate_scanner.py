from dataclasses import dataclass
from typing import Dict, Iterable, List, Tuple

import numpy as np
import pandas as pd
import imagehash


def _collect_hash_columns(df: pd.DataFrame) -> List[str]:
    return [c for c in df.columns if c.endswith("_hash")]


def _hash_to_bits(hash_hex: str) -> int:
    # Convert 16x16 aHash (default) hex to int bitstring
    try:
        return int(hash_hex, 16)
    except Exception:
        return 0


def _hamming_distance_int(a: int, b: int) -> int:
    return (a ^ b).bit_count()


@dataclass
class DuplicateMatch:
    image_a: str
    image_b: str
    job_a: str
    job_b: str
    hash_key_a: str
    hash_key_b: str
    distance: int
    similarity: float


class DuplicateScanner:
    """
    Efficient duplicate discovery using LSH-style banding on binary aHash values.
    Steps:
    - Convert each hash_hex to an integer bitstring
    - Band indexes (e.g., 8 bands of 16 bits if 128-bit hash) to build buckets
    - Candidate pairs are those sharing a bucket in any band
    - Validate candidates by true Hamming distance and threshold
    """

    def __init__(self, distance_threshold: int = 5, num_bands: int = 8):
        self.distance_threshold = distance_threshold
        self.num_bands = num_bands

    def _band_slices(self, total_bits: int) -> List[Tuple[int, int]]:
        # return list of (start, length) for bands
        band_size = total_bits // self.num_bands
        slices: List[Tuple[int, int]] = []
        start = 0
        for i in range(self.num_bands):
            length = band_size if i < self.num_bands - 1 else total_bits - start
            slices.append((start, length))
            start += length
        return slices

    def _band_keys(self, hash_int: int, total_bits: int) -> List[int]:
        keys: List[int] = []
        start = total_bits
        for (band_start, band_length) in self._band_slices(total_bits):
            # Extract band bits by shifting and masking
            shift = total_bits - (band_start + band_length)
            band_mask = (1 << band_length) - 1
            band_value = (hash_int >> shift) & band_mask
            keys.append(band_value)
        return keys

    def find_duplicates(self, df: pd.DataFrame) -> pd.DataFrame:
        hash_cols = _collect_hash_columns(df)
        if not hash_cols:
            return pd.DataFrame()

        # aHash default size is 8x8 (=64 bits) for imagehash.average_hash unless configured differently.
        # We can parse from any one hash by reconstructing the imagehash object.
        sample_hex = df[hash_cols[0]].dropna().astype(str).head(1)
        if sample_hex.empty:
            return pd.DataFrame()
        try:
            sample_hash = imagehash.hex_to_hash(sample_hex.iloc[0])
            total_bits = sample_hash.hash.size
        except Exception:
            total_bits = 64

        band_buckets: Dict[Tuple[str, int, int], List[int]] = {}
        # Map index to (hash_col, int_value) list for quick distance checks
        col_to_ints: Dict[str, List[int]] = {}
        for col in hash_cols:
            ints = df[col].fillna("").astype(str).map(_hash_to_bits).tolist()
            col_to_ints[col] = ints
            for idx, hv in enumerate(ints):
                if hv == 0:
                    continue
                for band_idx, band_key in enumerate(self._band_keys(hv, total_bits)):
                    bucket_key = (col, band_idx, band_key)
                    band_buckets.setdefault(bucket_key, []).append(idx)

        candidate_pairs: set[Tuple[int, int, str, str]] = set()
        for (_col, _band, _key), indices in band_buckets.items():
            if len(indices) < 2:
                continue
            indices_sorted = sorted(set(indices))
            for i in range(len(indices_sorted)):
                for j in range(i + 1, len(indices_sorted)):
                    a = indices_sorted[i]
                    b = indices_sorted[j]
                    candidate_pairs.add((a, b, _col, _col))

        # Also cross-compare across different hash columns by shared band key
        # This expands candidate set but still bounded by banding
        # Build quick lookup: (band_idx, band_key) -> {col: [indices]}
        cross_map: Dict[Tuple[int, int], Dict[str, List[int]]] = {}
        for (col, band_idx, key), idxs in band_buckets.items():
            cross_map.setdefault((band_idx, key), {}).setdefault(col, []).extend(idxs)
        for (band_idx, key), col_map in cross_map.items():
            cols = list(col_map.keys())
            for i in range(len(cols)):
                for j in range(i + 1, len(cols)):
                    ci, cj = cols[i], cols[j]
                    for a in set(col_map[ci]):
                        for b in set(col_map[cj]):
                            if a == b:
                                continue
                            candidate_pairs.add((min(a, b), max(a, b), ci, cj))

        matches: List[DuplicateMatch] = []
        # Validate candidates by true Hamming distance
        for a, b, col_i, col_j in candidate_pairs:
            hv_i = col_to_ints[col_i][a]
            hv_j = col_to_ints[col_j][b]
            if hv_i == 0 or hv_j == 0:
                continue
            dist = _hamming_distance_int(hv_i, hv_j)
            if dist <= self.distance_threshold:
                total_bits_local = total_bits
                similarity = 1.0 - (dist / total_bits_local)
                matches.append(
                    DuplicateMatch(
                        image_a=str(df.iloc[a]["image_name"]),
                        image_b=str(df.iloc[b]["image_name"]),
                        job_a=str(df.iloc[a]["job_number"]),
                        job_b=str(df.iloc[b]["job_number"]),
                        hash_key_a=col_i,
                        hash_key_b=col_j,
                        distance=dist,
                        similarity=similarity,
                    )
                )

        if not matches:
            return pd.DataFrame()
        out = pd.DataFrame([m.__dict__ for m in matches])
        out = out.sort_values(["distance", "image_a", "image_b"]).reset_index(drop=True)
        return out


