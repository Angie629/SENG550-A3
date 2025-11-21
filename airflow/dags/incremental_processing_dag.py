"""
Part 2 - Airflow DAG for Incremental Data Processing
Runs every 4 seconds to process new data incrementally with Redis tracking
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator
import redis
import os
from pathlib import Path

# DAG default arguments
default_args = {
    'owner': 'data-engineer',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(seconds=5),
}

# Create DAG
dag = DAG(
    'incremental_data_processing',
    default_args=default_args,
    description='Process incremental orders data with Redis tracking',
    schedule_interval=timedelta(seconds=4),  # Run every 4 seconds
    catchup=False,
    max_active_runs=1,  # Prevent overlapping runs
    tags=['spark', 'incremental', 'redis']
)

def check_redis_connection():
    """Check if Redis is available and reset if needed"""
    try:
        r = redis.Redis(host='redis', port=6379, db=0, decode_responses=True)
        r.ping()
        print("Redis connection successful")
        
        # Log current state
        processed_days = r.smembers("processed_days")
        last_processed = r.get("last_processed_day")
        print(f"Currently processed days: {sorted(processed_days)}")
        print(f"Last processed day: {last_processed}")
        
        return True
    except Exception as e:
        print(f"Redis connection failed: {e}")
        return False

def simulate_new_data():
    """Simulate arrival of new data by copying files"""
    try:
        workspace = "/workspace"
        source_base = f"{workspace}/part1/data/raw"
        target_base = f"{workspace}/data/incremental/raw"
        
        # Check what day directories we can simulate
        available_source_days = []
        for i in range(7):  # Days 0-6 available from original split
            source_path = Path(f"{source_base}/{i}")
            if source_path.exists():
                available_source_days.append(i)
        
        # Check what's already been simulated
        existing_target_days = []
        target_path = Path(target_base)
        if target_path.exists():
            for day_dir in target_path.iterdir():
                if day_dir.is_dir() and day_dir.name.isdigit():
                    existing_target_days.append(int(day_dir.name))
        
        print(f"Available source days: {available_source_days}")
        print(f"Existing target days: {existing_target_days}")
        
        # Simulate new data arrival occasionally (simulate days 3-6 as new arrivals)
        import random
        if random.random() < 0.3:  # 30% chance of new data arrival
            next_day = max(existing_target_days, default=-1) + 1
            if next_day <= 6:  # Only up to day 6
                source_day = next_day
                if source_day in available_source_days:
                    source_file = f"{source_base}/{source_day}/orders_{source_day}.csv"
                    target_dir = f"{target_base}/{next_day}"
                    target_file = f"{target_dir}/orders_{next_day}.csv"
                    
                    # Create directory and copy file
                    os.makedirs(target_dir, exist_ok=True)
                    
                    # Use system copy command
                    import shutil
                    shutil.copy2(source_file, target_file)
                    print(f"Simulated new data arrival: day {next_day}")
                else:
                    print(f"No source data for day {next_day}")
            else:
                print("All days have been simulated")
        else:
            print("No new data this cycle")
            
    except Exception as e:
        print(f"Error simulating new data: {e}")

# Task 1: Check Redis connection
check_redis_task = PythonOperator(
    task_id='check_redis_connection',
    python_callable=check_redis_connection,
    dag=dag,
)

# Task 2: Simulate new data arrival
simulate_data_task = PythonOperator(
    task_id='simulate_new_data_arrival',
    python_callable=simulate_new_data,
    dag=dag,
)

# Task 3: Run incremental Spark processing
spark_processing_task = BashOperator(
    task_id='run_incremental_spark_processing',
    bash_command="""
    cd /workspace && \
    /opt/bitnami/spark/bin/spark-submit \
        --master spark://spark-master:7077 \
        --conf spark.sql.adaptive.enabled=true \
        --conf spark.sql.adaptive.coalescePartitions.enabled=true \
        /workspace/processing/incremental/incremental_aggregation.py
    """,
    dag=dag,
)

# Task 4: Log processing status
def log_processing_status():
    """Log the current processing status"""
    try:
        r = redis.Redis(host='redis', port=6379, db=0, decode_responses=True)
        processed_days = r.smembers("processed_days")
        last_processed = r.get("last_processed_day")
        
        print("=== PROCESSING STATUS ===")
        print(f"Processed days: {sorted(processed_days)}")
        print(f"Last processed day: {last_processed}")
        print(f"Total processed days: {len(processed_days)}")
        
        # Check for available but unprocessed days
        workspace = "/workspace/data/incremental/raw"
        available_days = []
        if os.path.exists(workspace):
            for item in os.listdir(workspace):
                if os.path.isdir(os.path.join(workspace, item)) and item.isdigit():
                    available_days.append(int(item))
        
        unprocessed = set(available_days) - set(int(d) for d in processed_days)
        print(f"Available days: {sorted(available_days)}")
        print(f"Unprocessed days: {sorted(unprocessed)}")
        
    except Exception as e:
        print(f"Error logging status: {e}")

log_status_task = PythonOperator(
    task_id='log_processing_status',
    python_callable=log_processing_status,
    dag=dag,
)

# Set task dependencies
check_redis_task >> simulate_data_task >> spark_processing_task >> log_status_task