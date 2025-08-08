import io
from dataclasses import dataclass
from typing import Dict, Iterable, Iterator, List, Optional, Tuple

import boto3
import botocore.exceptions as botocore_exceptions
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
        session_kwargs: Dict[str, str] = {}
        if profile_name:
            session_kwargs["profile_name"] = profile_name
        base_session = boto3.Session(**session_kwargs)
        if assume_role_arn:
            sts = base_session.client("sts", region_name=region_name)
            assume_args = {"RoleArn": assume_role_arn, "RoleSessionName": "siteTrackerDuplicateDetection"}
            if external_id:
                assume_args["ExternalId"] = external_id
            creds = sts.assume_role(**assume_args)["Credentials"]
            self._s3 = boto3.client(
                "s3",
                region_name=region_name,
                aws_access_key_id=creds["AccessKeyId"],
                aws_secret_access_key=creds["SecretAccessKey"],
                aws_session_token=creds["SessionToken"],
            )
            self._session = base_session
        else:
            self._session = base_session
            self._s3 = self._session.client("s3", region_name=region_name)
        self._log = get_logger("s3_client")

    def iter_job_prefixes(self, bucket: str, root_prefix: str) -> Iterator[str]:
        """
        Yields job-level prefixes immediately under the provided root prefix, using delimiter '/'.
        """
        if root_prefix and not root_prefix.endswith("/"):
            root_prefix = root_prefix + "/"
        paginator = self._s3.get_paginator("list_objects_v2")
        try:
            for page in paginator.paginate(Bucket=bucket, Prefix=root_prefix, Delimiter="/"):
                for cp in page.get("CommonPrefixes", []):
                    prefix = cp.get("Prefix")
                    if prefix:
                        yield prefix
        except botocore_exceptions.ClientError as e:
            code = e.response.get("Error", {}).get("Code")
            self._log.error(
                f"Failed to list job prefixes for s3://{bucket}/{root_prefix} (ErrorCode={code}). "
                f"Ensure bucket, prefix, region and permissions are correct."
            )
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
        paginator = self._s3.get_paginator("list_objects_v2")
        for page in paginator.paginate(Bucket=bucket, Prefix=job_prefix):
            for obj in page.get("Contents", []):
                key = obj["Key"]
                if key.endswith("url.txt"):
                    continue
                # Filter out folder placeholders
                if key.endswith("/"):
                    continue
                yield key, obj

    def stream_bytes(self, bucket: str, key: str) -> bytes:
        obj = self._s3.get_object(Bucket=bucket, Key=key)
        return obj["Body"].read()

    def list_s3_images_with_metadata(
        self,
        bucket: str,
        root_prefix: str,
        image_extensions: Tuple[str, ...] = (".png", ".jpg", ".jpeg", ".bmp", ".gif"),
    ) -> List[S3Object]:
        """
        Returns metadata for all images under root_prefix, capturing job_number from the top-level folder
        and job_url from that folder's url.txt.
        """
        results: List[S3Object] = []
        for job_prefix in self.iter_job_prefixes(bucket, root_prefix):
            # job_number is the last component without trailing slash
            job_number = job_prefix.rstrip("/").split("/")[-1]
            job_url = self.get_job_url(bucket, job_prefix)
            for key, meta in self.iter_images_in_job(bucket, job_prefix):
                lower = key.lower()
                if not lower.endswith(image_extensions):
                    continue
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
        return results


