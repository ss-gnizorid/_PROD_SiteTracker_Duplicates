### SiteTracker Image Hash Indexer

Build a reproducible hash index of images stored in Amazon S3. The index is written as CSV or Parquet for ingestion by downstream systems (e.g., Microsoft Fabric). Duplicate detection is performed in the cloud; this project focuses solely on scanning S3 and producing high-quality perceptual hashes.

### Key capabilities
- Enumerates jobs under an S3 root prefix and streams image content
- Extracts job metadata (`job_number`, optional `job_url` from `url.txt` per job folder)
- Generates robust perceptual hashes (phash) across simple permutations
- Writes a deduplicated index locally (CSV/Parquet)
- Maintains incremental state to process only new/changed images on subsequent runs

### Non‑goals
- Local duplicate detection (handled downstream)
- Local databases (SQLite) or data warehouse delivery (Redshift/Fabric)

### Architecture
- `scripts/main/build_hash_index.py`: entry point to build/update the hash index
- `src/clients/aws_client.py`: S3 listing, metadata extraction, image streaming
- `src/tools/permutation_generator.py`: phash computation for image permutations
- `src/tools/hash_indexer.py`: concurrent hashing orchestration into a DataFrame
- `src/config/config.py`: configuration model and YAML loader
- `src/utils/state.py`: incremental state store (S3 key + ETag)
- `src/utils/io.py`: DataFrame writers (CSV/Parquet)
- `scripts/main/generate_presigned_links.py`: decoupled step to generate presigned URLs
  (7-day expiry by default) and maintain link state for incremental refreshes

### Prerequisites
- Python 3.12+
- AWS credentials available via one of the standard mechanisms:
  - Environment variables (AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY, AWS_SESSION_TOKEN)
  - AWS shared config/credentials files (e.g., `~/.aws/config`, `~/.aws/credentials`) with `profile`
  - SSO-backed profiles
  - Optional assume-role (via `aws.assume_role_arn` and `aws.external_id` in YAML)

### Installation
- Create/activate a virtual environment
- Install dependencies:
  - Using uv: `uv pip install -r pyproject.toml`
  - Or pip: `pip install -e .` (ensure `pyproject.toml` dependencies are satisfied)

### Configuration
Edit `configs/main_config.yaml`. The file is the single source of truth for runtime settings.

Example:
```yaml
aws:
  region: "ap-southeast-2"
  profile: null         # e.g., "default" or an SSO profile
  # assume_role_arn: "arn:aws:iam::123456789012:role/YourRole"  # optional
  # external_id: "your-external-id"                             # optional

s3:
  bucket: "prod-sitetracker-image-store"
  root_prefix: "ODM Duplicate Image/"
  timeout_seconds: 300
  # max_jobs_to_process: 10   # optional, limit jobs for test runs

hashing:
  mode: "basic"      # "basic" permutations; "advanced" adds minor rotations/zoom/brightness/contrast
  resize:
    width: 256
    height: 256

output:
  target: "local_csv" # "local_csv" or "local_parquet"
  path: 'C:\\Users\\George.Nizoridis\\OneDrive - Service Stream\\Documents - Analytics & Insights Team\\02 Telco\\Projects\\SiteTracker - Duplicate Images\\Hash_DB_Store\\hash_index.csv'

state:
  path: "outputs/state/seen.json"

links:
  enabled: true
  expiry_days: 7
  output:
    target: "local_parquet"   # or "local_csv"
    path: "outputs/links/link_index.parquet"
  state_path: "outputs/state/links.json"
  workers: 16
```

Notes
- To point this tool at a different S3 bucket/prefix, update `s3.bucket` and `s3.root_prefix` accordingly.
- For Windows paths in YAML, prefer single quotes and escaped backslashes as shown above.
- The incremental state file ensures subsequent runs skip unchanged images (based on ETag). Delete it to force a full rebuild.

### Running
- From the project root:
  ```bash
  python scripts/main/build_hash_index.py --config configs/main_config.yaml
  ```
- Or via the main entry:
  ```bash
  python main.py --config configs/main_config.yaml
  ```

- Generate or refresh presigned links (decoupled step):
  ```bash
  python scripts/main/generate_presigned_links.py --config configs/main_config.yaml
  ```

Expected logging
- Job prefixes discovered under the root prefix
- Count of images to process (new/changed)
- Hashing progress and final output path
- State update confirmation

### Output
The index is written to the configured `output.path` with the chosen format:
- `local_csv` → `<path>.csv`
- `local_parquet` → `<path>.parquet`

Schema (columns)
- `image_name` (string): S3 object key
- `job_number` (string): last folder segment under the root prefix
- `job_url` (string): contents of `<job_prefix>/url.txt`, if present; otherwise empty
- Hash columns (strings, hex-encoded):
  - `original_hash`
  - `h_flip_hash`
  - `v_flip_hash`
  - Additional columns appear when `hashing.mode = advanced` (e.g., small rotations/zoom/brightness/contrast variants)

### Hashing behaviour
- Algorithm: perceptual hash (`phash`, via `imagehash`)
- Default bit-length: 64-bit (imagehash default)
- Preprocessing: resize to configured dimensions (default 256x256), convert to grayscale
- Permutations:
  - `basic`: original, horizontal flip, vertical flip
  - `advanced`: adds slight rotations, mild zoom, and small brightness/contrast adjustments

To increase hash bit-length (e.g., to 256-bit), adjust `src/tools/permutation_generator.py` to call `imagehash.phash(img, hash_size=16)` and ensure downstream consumers can handle longer hex strings.

### Operational guidance
- Limit scope for test runs: set `s3.max_jobs_to_process` in YAML
- Change format: set `output.target` to `local_parquet`
- Force reprocessing: delete the state file configured at `state.path`
- Control concurrency: adjust `hashing.workers` in YAML (maps to `max_workers`)

Link generation
- Generates presigned GET URLs with 7-day expiry by default
- Maintains a separate link state at `links.state_path` for incremental refresh
- Writes a link index to `links.output.path` with columns:
  - `image_name`, `presigned_url`, `generated_at`, `expires_at`

### Troubleshooting
- No new output / nothing happens: check logs for "No new images to process." Remove the state file to force reprocessing.
- Permission or credential errors: verify AWS profile/role/SSO settings and region.
- OneDrive path issues: verify the path exists and is writable; for YAML on Windows, keep the quoting/escaping as shown.
- Slow or large runs: use `s3.max_jobs_to_process` to validate with a small subset first.

### Contributing
- Follow existing code style (type hints, clear naming, early returns) and keep functions focused
- Add/update README and configuration examples for any user-facing changes
- Ensure logging remains informative at INFO level; avoid chatty DEBUG by default


