import io
from dataclasses import dataclass
from typing import Dict, Iterable, Iterator, List, Optional, Tuple

import boto3
import botocore
from botocore import exceptions as botocore_exceptions
from src.utils.logger import get_logger


@dataclass
class S3Object:
    bucket: str
    key: str
    etag: str
    size: int
    last_modified: str
    job_number: str
    job_url: Optional[str]


class S3Client:
    """
    Thin wrapper around boto3 S3 for listing job folders, reading url.txt, and streaming image bytes.
    Credentials resolution is delegated to the default AWS chain (env vars, profile, role, etc.).
    """

    def __init__(
        self,
        region_name: Optional[str] = None,
        profile_name: Optional[str] = None,
        assume_role_arn: Optional[str] = None,
        external_id: Optional[str] = None,
    ):
        base_session = boto3.Session(profile_name=profile_name)
        if assume_role_arn:
            sts = base_session.client("sts")
            assume_role_kwargs = {
                "RoleArn": assume_role_arn,
                "RoleSessionName": "s3_client_session",
            }
            if external_id:
                assume_role_kwargs["ExternalId"] = external_id
            assumed_role = sts.assume_role(**assume_role_kwargs)
            base_session = boto3.Session(
                aws_access_key_id=assumed_role["Credentials"]["AccessKeyId"],
                aws_secret_access_key=assumed_role["Credentials"]["SecretAccessKey"],
                aws_session_token=assumed_role["Credentials"]["SessionToken"],
            )
            self._session = base_session
        else:
            self._session = base_session
        
        # Configure client with timeouts to prevent hanging
        config = botocore.config.Config(
            connect_timeout=30,  # 30 seconds to establish connection
            read_timeout=60,     # 60 seconds to read response
            retries={'max_attempts': 3}  # Retry failed requests up to 3 times
        )
        self._s3 = self._session.client("s3", region_name=region_name, config=config)
        self._log = get_logger("s3_client")

    def iter_job_prefixes(self, bucket: str, root_prefix: str) -> Iterator[str]:
        """
        Yields job-level prefixes immediately under the provided root prefix, using delimiter '/'.
        """
        if root_prefix and not root_prefix.endswith("/"):
            root_prefix = root_prefix + "/"
        
        self._log.info(f"Listing job prefixes in s3://{bucket}/{root_prefix}")
        paginator = self._s3.get_paginator("list_objects_v2")
        
        try:
            page_count = 0
            for page in paginator.paginate(Bucket=bucket, Prefix=root_prefix, Delimiter="/"):
                page_count += 1
                self._log.debug(f"Processing page {page_count} of job prefixes")
                
                for cp in page.get("CommonPrefixes", []):
                    prefix = cp.get("Prefix")
                    if prefix:
                        self._log.debug(f"Found job prefix: {prefix}")
                        yield prefix
                        
        except botocore_exceptions.ClientError as e:
            code = e.response.get("Error", {}).get("Code")
            self._log.error(
                f"Failed to list job prefixes for s3://{bucket}/{root_prefix} (ErrorCode={code}). "
                f"Ensure bucket, prefix, region and permissions are correct."
            )
            raise
        except Exception as e:
            self._log.error(f"Unexpected error listing job prefixes: {e}")
            raise

    def read_text_object(self, bucket: str, key: str, encoding: str = "utf-8") -> Optional[str]:
        try:
            obj = self._s3.get_object(Bucket=bucket, Key=key)
        except self._s3.exceptions.NoSuchKey:
            return None
        body = obj["Body"].read()
        return body.decode(encoding)

    def get_job_url(self, bucket: str, job_prefix: str) -> Optional[str]:
        if not job_prefix.endswith("/"):
            job_prefix += "/"
        url_key = f"{job_prefix}url.txt"
        url = self.read_text_object(bucket, url_key)
        if not url:
            self._log.debug(f"No url.txt at {bucket}/{url_key}")
        return url

    def iter_images_in_job(self, bucket: str, job_prefix: str) -> Iterator[Tuple[str, Dict]]:
        """
        Yields (key, object_summary_dict) for each object under job_prefix excluding the url.txt.
        """
        if not job_prefix.endswith("/"):
            job_prefix += "/"
        
        self._log.debug(f"Listing images in job: {job_prefix}")
        paginator = self._s3.get_paginator("list_objects_v2")
        
        try:
            page_count = 0
            for page in paginator.paginate(Bucket=bucket, Prefix=job_prefix):
                page_count += 1
                self._log.debug(f"Processing page {page_count} for job {job_prefix}")
                
                for obj in page.get("Contents", []):
                    key = obj["Key"]
                    if key.endswith("url.txt"):
                        continue
                    # Filter out folder placeholders
                    if key.endswith("/"):
                        continue
                    yield key, obj
                    
        except Exception as e:
            self._log.error(f"Error listing images in job {job_prefix}: {e}")
            raise

    def stream_bytes(self, bucket: str, key: str) -> bytes:
        obj = self._s3.get_object(Bucket=bucket, Key=key)
        return obj["Body"].read()

    def list_s3_images_with_metadata(
        self,
        bucket: str,
        root_prefix: str,
        image_extensions: Tuple[str, ...] = (".png", ".jpg", ".jpeg", ".bmp", ".gif"),
        max_jobs_to_process: Optional[int] = None,
    ) -> List[S3Object]:
        """
        Returns metadata for all images under root_prefix, capturing job_number from the top-level folder
        and job_url from that folder's url.txt.
        
        Args:
            bucket: S3 bucket name
            root_prefix: Root prefix to search under
            image_extensions: File extensions to include
            max_jobs_to_process: Maximum number of jobs to process (for testing/debugging)
        """
        results: List[S3Object] = []
        job_count = 0
        
        try:
            for job_prefix in self.iter_job_prefixes(bucket, root_prefix):
                job_count += 1
                self._log.info(f"Processing job {job_count}: {job_prefix}")
                
                # Check if we've hit the job limit
                if max_jobs_to_process and job_count > max_jobs_to_process:
                    self._log.info(f"Reached job limit of {max_jobs_to_process}, stopping processing")
                    break
                
                # job_number is the last component without trailing slash
                job_number = job_prefix.rstrip("/").split("/")[-1]
                job_url = self.get_job_url(bucket, job_prefix)
                
                image_count = 0
                for key, meta in self.iter_images_in_job(bucket, job_prefix):
                    lower = key.lower()
                    if not lower.endswith(image_extensions):
                        continue
                    
                    image_count += 1
                    if image_count % 100 == 0:
                        self._log.debug(f"Processed {image_count} images in job {job_number}")
                    
                    results.append(
                        S3Object(
                            bucket=bucket,
                            key=key,
                            etag=meta.get("ETag", "").strip('"'),
                            size=int(meta.get("Size", 0)),
                            last_modified=str(meta.get("LastModified", "")),
                            job_number=job_number,
                            job_url=job_url,
                        )
                    )
                
                self._log.info(f"Found {image_count} images in job {job_number}")
                
        except KeyboardInterrupt:
            self._log.warning("S3 listing interrupted by user")
            raise
        except Exception as e:
            self._log.error(f"Error during S3 listing: {e}")
            raise
            
        self._log.info(f"Total: {len(results)} images found across {job_count} jobs")
        return results


