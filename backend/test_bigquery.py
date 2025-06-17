#!/usr/bin/env python3
"""
Test script to verify BigQuery connection is working properly.
Run this script to test if the GCP credentials are configured correctly.
"""

import os
import sys
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Import the BigQuery service
from services.bigquery_service import client, get_table

def test_bigquery_connection():
    """Test the BigQuery connection and basic operations."""
    try:
        print("🔍 Testing BigQuery connection...")
        
        # Test 1: Check if client is initialized
        if client is None:
            print("❌ BigQuery client is not initialized")
            return False
        
        print(f"✅ BigQuery client initialized successfully")
        print(f"   Project ID: {client.project}")
        
        # Test 2: Test table reference creation
        try:
            table_ref = get_table("groups", "student")
            print(f"✅ Table reference created: {table_ref.project}.{table_ref.dataset_id}.{table_ref.table_id}")
        except Exception as e:
            print(f"❌ Failed to create table reference: {e}")
            return False
        
        # Test 3: Test basic query (just check if we can connect)
        try:
            query = "SELECT 1 as test"
            query_job = client.query(query)
            results = query_job.result()
            print("✅ Basic query executed successfully")
        except Exception as e:
            print(f"❌ Failed to execute basic query: {e}")
            return False
        
        print("🎉 All BigQuery tests passed!")
        return True
        
    except Exception as e:
        print(f"❌ BigQuery connection test failed: {e}")
        return False

def check_environment():
    """Check if environment variables are set correctly."""
    print("🔍 Checking environment configuration...")
    
    # Check for GCP_SERVICE_ACCOUNT_JSON
    gcp_json = os.getenv('GCP_SERVICE_ACCOUNT_JSON')
    if gcp_json:
        print("✅ GCP_SERVICE_ACCOUNT_JSON is set")
    else:
        print("❌ GCP_SERVICE_ACCOUNT_JSON is not set")
    
    # Check for GOOGLE_APPLICATION_CREDENTIALS
    gcp_creds = os.getenv('GOOGLE_APPLICATION_CREDENTIALS')
    if gcp_creds:
        print(f"✅ GOOGLE_APPLICATION_CREDENTIALS is set: {gcp_creds}")
    else:
        print("❌ GOOGLE_APPLICATION_CREDENTIALS is not set")
    
    # Check if .env file exists
    if os.path.exists('.env'):
        print("✅ .env file exists")
    else:
        print("❌ .env file does not exist")

if __name__ == "__main__":
    print("🚀 Starting BigQuery Connection Test\n")
    
    check_environment()
    print()
    
    success = test_bigquery_connection()
    
    if success:
        print("\n🎉 BigQuery is configured correctly!")
        sys.exit(0)
    else:
        print("\n💥 BigQuery configuration has issues!")
        sys.exit(1) 