#!/usr/bin/env python3
"""
Test script to verify S3 connectivity and identify hanging issues.
"""

import sys
import time
from pathlib import Path

# Add the project root to the path so we can import src modules
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

def test_s3_connection():
    """Test S3 connection with detailed debugging steps."""
    print("🔍 Starting S3 connection test...")
    
    # Step 1: Test config loading
    print("\n📋 Step 1: Loading configuration...")
    try:
        from src.config.config import load_config_yaml
        config_path = project_root / "configs/main_config.yaml"
        if not config_path.exists():
            print(f"❌ Config file not found: {config_path}")
            return False
            
        cfg = load_config_yaml(config_path)
        print(f"✅ Config loaded - bucket: {cfg.s3_bucket}, region: {cfg.aws_region}")
    except Exception as e:
        print(f"❌ Failed to load config: {e}")
        return False
    
    # Step 2: Test logger initialization
    print("\n📝 Step 2: Initializing logger...")
    try:
        from src.utils.logger import get_logger
        log = get_logger("test_s3")
        print("✅ Logger initialized")
    except Exception as e:
        print(f"❌ Failed to initialize logger: {e}")
        return False
    
    # Step 3: Test boto3 import and basic setup
    print("\n🐍 Step 3: Testing boto3 setup...")
    try:
        import boto3
        import botocore
        print("✅ boto3 and botocore imported successfully")
        
        # Test basic session creation
        session = boto3.Session(profile_name=cfg.aws_profile)
        print("✅ boto3 session created")
    except Exception as e:
        print(f"❌ Failed boto3 setup: {e}")
        return False
    
    # Step 4: Test S3Client initialization
    print("\n🔧 Step 4: Initializing S3Client...")
    try:
        from src.clients.aws_client import S3Client
        print("✅ S3Client class imported")
        
        start_time = time.time()
        s3 = S3Client(
            region_name=cfg.aws_region,
            profile_name=cfg.aws_profile,
            assume_role_arn=cfg.aws_assume_role_arn,
            external_id=cfg.aws_external_id,
        )
        init_time = time.time() - start_time
        print(f"✅ S3Client initialized in {init_time:.2f}s")
    except Exception as e:
        print(f"❌ Failed to initialize S3Client: {e}")
        return False
    
    # Step 5: Test basic S3 operation (list buckets)
    print("\n🪣 Step 5: Testing basic S3 operation (list buckets)...")
    try:
        start_time = time.time()
        response = s3._s3.list_buckets()
        list_time = time.time() - start_time
        print(f"✅ List buckets successful in {list_time:.2f}s")
        print(f"   Found {len(response['Buckets'])} buckets")
    except Exception as e:
        print(f"❌ Failed to list buckets: {e}")
        return False
    
    # Step 6: Test bucket access
    print(f"\n🔍 Step 6: Testing bucket access: {cfg.s3_bucket}")
    try:
        start_time = time.time()
        response = s3._s3.head_bucket(Bucket=cfg.s3_bucket)
        head_time = time.time() - start_time
        print(f"✅ Bucket access successful in {head_time:.2f}s")
    except Exception as e:
        print(f"❌ Failed to access bucket: {e}")
        return False
    
    # Step 7: Test prefix listing with timeout
    print(f"\n📁 Step 7: Testing prefix listing: {cfg.s3_root_prefix}")
    try:
        start_time = time.time()
        # Use a very small limit and timeout
        objects = s3.list_s3_images_with_metadata(
            cfg.s3_bucket,
            cfg.s3_root_prefix,
            max_jobs_to_process=1  # Only 1 job for testing
        )
        list_time = time.time() - start_time
        print(f"✅ Prefix listing successful in {list_time:.2f}s")
        print(f"   Found {len(objects)} objects")
        
        if objects:
            print("   Sample objects:")
            for i, obj in enumerate(objects[:3]):
                print(f"     {i+1}. {obj.key} (job: {obj.job_number})")
        
        return True
        
    except Exception as e:
        print(f"❌ Failed to list prefix: {e}")
        return False

if __name__ == "__main__":
    print("🚀 Starting S3 Connection Test")
    print("=" * 50)
    
    success = test_s3_connection()
    
    print("\n" + "=" * 50)
    if success:
        print("🎉 S3 connection test PASSED")
        sys.exit(0)
    else:
        print("💥 S3 connection test FAILED")
        sys.exit(1)
