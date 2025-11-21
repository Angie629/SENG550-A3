"""
Part 4 - Airflow DAG for Redis Cache Refresh
Runs every 20 seconds to refresh prediction cache in Redis
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator
import redis
import json

# DAG default arguments
default_args = {
    'owner': 'cache-engineer',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(seconds=10),
}

# Create DAG
dag = DAG(
    'redis_cache_refresh_pipeline',
    default_args=default_args,
    description='Refresh Redis prediction cache every 20 seconds',
    schedule_interval=timedelta(seconds=20),  # Run every 20 seconds
    catchup=False,
    max_active_runs=1,  # Prevent overlapping runs
    tags=['redis', 'cache', 'predictions']
)

def check_cache_status():
    """Check current Redis cache status"""
    try:
        r = redis.Redis(host='redis', port=6379, db=1, decode_responses=True)
        r.ping()
        
        # Get cache metadata
        metadata = r.get("cache_metadata")
        if metadata:
            metadata_obj = json.loads(metadata)
            print("Current cache metadata:")
            for key, value in metadata_obj.items():
                print(f"  {key}: {value}")
            
            # Count current cached predictions
            prediction_keys = r.keys("*:*:*")
            print(f"Currently cached predictions: {len(prediction_keys)}")
            
            return True
        else:
            print("No cache metadata found - cache may be empty")
            return False
            
    except Exception as e:
        print(f"Error checking cache status: {e}")
        return False

def verify_model_availability():
    """Check if the ML model is available for prediction generation"""
    import os
    
    try:
        workspace = "/workspace"
        model_path = f"{workspace}/processing/ml/trained_model"
        
        if os.path.exists(model_path):
            # Check if model files exist
            model_files = []
            for root, dirs, files in os.walk(model_path):
                model_files.extend(files)
            
            if model_files:
                print(f"Model available: {len(model_files)} files found")
                return True
            else:
                print("Model directory exists but no model files found")
                return False
        else:
            print("Model directory not found")
            return False
    
    except Exception as e:
        print(f"Error checking model availability: {e}")
        return False

def validate_cache_refresh():
    """Validate that cache was refreshed successfully"""
    try:
        r = redis.Redis(host='redis', port=6379, db=1, decode_responses=True)
        
        # Get updated metadata
        metadata = r.get("cache_metadata")
        if metadata:
            metadata_obj = json.loads(metadata)
            last_updated = metadata_obj.get('last_updated', 'unknown')
            total_predictions = metadata_obj.get('total_predictions', 0)
            
            print(f"Cache refresh validation:")
            print(f"  Last updated: {last_updated}")
            print(f"  Total predictions: {total_predictions}")
            
            # Test some sample lookups
            test_keys = [
                "0:9:coffee",
                "1:18:fresh_fruits", 
                "5:12:milk"
            ]
            
            successful_lookups = 0
            for key in test_keys:
                value = r.get(key)
                if value:
                    prediction_data = json.loads(value)
                    print(f"  Test lookup {key}: {prediction_data['prediction']} items")
                    successful_lookups += 1
                else:
                    print(f"  Test lookup {key}: NOT FOUND")
            
            success_rate = successful_lookups / len(test_keys)
            print(f"  Test lookup success rate: {success_rate:.1%}")
            
            return success_rate > 0.8  # At least 80% success rate
        else:
            print("Cache validation failed - no metadata found")
            return False
    
    except Exception as e:
        print(f"Error validating cache refresh: {e}")
        return False

def log_cache_performance():
    """Log cache performance metrics"""
    try:
        r = redis.Redis(host='redis', port=6379, db=1, decode_responses=True)
        
        # Performance test - measure lookup time
        import time
        
        test_key = "0:9:coffee"
        start_time = time.time()
        
        for _ in range(100):  # 100 lookups
            value = r.get(test_key)
        
        end_time = time.time()
        avg_lookup_time = (end_time - start_time) / 100 * 1000  # Convert to milliseconds
        
        print(f"=== CACHE PERFORMANCE METRICS ===")
        print(f"Average lookup time: {avg_lookup_time:.2f} ms")
        
        # Memory usage
        memory_info = r.info('memory')
        used_memory = memory_info.get('used_memory_human', 'unknown')
        print(f"Redis memory usage: {used_memory}")
        
        # Key count
        total_keys = r.dbsize()
        print(f"Total keys in cache DB: {total_keys}")
        
        # Cache hit simulation
        sample_keys = r.keys("*:*:*")[:10]
        hits = 0
        for key in sample_keys:
            if r.exists(key):
                hits += 1
        
        if sample_keys:
            hit_rate = hits / len(sample_keys)
            print(f"Sample hit rate: {hit_rate:.1%}")
        
        return True
    
    except Exception as e:
        print(f"Error logging cache performance: {e}")
        return False

# Task 1: Check current cache status
check_cache_task = PythonOperator(
    task_id='check_current_cache_status',
    python_callable=check_cache_status,
    dag=dag,
)

# Task 2: Verify model availability
verify_model_task = PythonOperator(
    task_id='verify_model_availability',
    python_callable=verify_model_availability,
    dag=dag,
)

# Task 3: Run batch prediction and cache refresh
cache_refresh_task = BashOperator(
    task_id='run_batch_prediction_caching',
    bash_command="""
    cd /workspace && \
    /opt/bitnami/spark/bin/spark-submit \
        --master spark://spark-master:7077 \
        --conf spark.sql.adaptive.enabled=true \
        --conf spark.serializer=org.apache.spark.serializer.KryoSerializer \
        --packages org.apache.spark:spark-mllib_2.12:3.5.0 \
        /workspace/processing/ml/batch_prediction_cache.py
    """,
    dag=dag,
)

# Task 4: Validate cache refresh
validate_cache_task = PythonOperator(
    task_id='validate_cache_refresh',
    python_callable=validate_cache_refresh,
    dag=dag,
)

# Task 5: Log performance metrics
performance_task = PythonOperator(
    task_id='log_cache_performance_metrics',
    python_callable=log_cache_performance,
    dag=dag,
)

# Set task dependencies
check_cache_task >> verify_model_task >> cache_refresh_task >> validate_cache_task >> performance_task