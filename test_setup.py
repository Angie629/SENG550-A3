#!/usr/bin/env python3
"""
Test script to verify the setup is working
"""

import requests
import time
import redis
import sys

def check_spark():
    """Check if Spark Master is running"""
    try:
        response = requests.get("http://localhost:8080", timeout=5)
        if "Spark Master" in response.text:
            print("✅ Spark Master is running at http://localhost:8080")
            return True
        else:
            print("⚠️  Spark Master response unexpected")
            return False
    except Exception as e:
        print(f"❌ Spark Master not accessible: {e}")
        return False

def check_airflow():
    """Check if Airflow is running"""
    try:
        response = requests.get("http://localhost:8081", timeout=10)
        if response.status_code == 200:
            print("✅ Airflow Web UI is accessible at http://localhost:8081")
            print("   Login with: admin / admin")
            return True
        else:
            print(f"⚠️  Airflow returned status code: {response.status_code}")
            return False
    except Exception as e:
        print(f"❌ Airflow not accessible: {e}")
        return False

def check_redis():
    """Check if Redis is running"""
    try:
        r = redis.Redis(host='localhost', port=6379, db=0, decode_responses=True)
        r.ping()
        print("✅ Redis is accessible at localhost:6379")
        return True
    except Exception as e:
        print(f"❌ Redis not accessible: {e}")
        return False

def main():
    print("=== SENG550-A3 System Health Check ===")
    print()
    
    all_good = True
    
    # Check services
    all_good &= check_spark()
    all_good &= check_airflow()  
    all_good &= check_redis()
    
    print()
    if all_good:
        print("🎉 All services are running successfully!")
        print()
        print("Next steps:")
        print("1. Access Airflow at http://localhost:8081")
        print("2. Login with admin / admin")
        print("3. Enable the DAGs:")
        print("   - incremental_processing_dag")
        print("   - ml_training_dag") 
        print("   - redis_cache_dag")
        print("4. Monitor the processing in real-time")
    else:
        print("❌ Some services are not ready yet. Wait a few minutes and try again.")
        sys.exit(1)

if __name__ == "__main__":
    main()