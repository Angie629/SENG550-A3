#!/usr/bin/env python3
"""
Part 2 - Incremental Data Aggregation with Redis Tracking
Modified Spark job that processes only unprocessed days and tracks progress in Redis
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum as spark_sum, lit
import redis
import os
import sys
from pathlib import Path

class IncrementalProcessor:
    def __init__(self, redis_host="redis", redis_port=6379, redis_db=0):
        """Initialize the incremental processor with Redis connection"""
        self.redis_client = redis.Redis(host=redis_host, port=redis_port, db=redis_db, decode_responses=True)
        self.processed_key = "processed_days"
        self.last_processed_key = "last_processed_day"
        
    def get_processed_days(self):
        """Get set of already processed days from Redis"""
        try:
            processed = self.redis_client.smembers(self.processed_key)
            return set(int(day) for day in processed)
        except Exception as e:
            print(f"Error reading from Redis: {e}")
            return set()
    
    def mark_day_processed(self, day):
        """Mark a day as processed in Redis"""
        try:
            self.redis_client.sadd(self.processed_key, str(day))
            self.redis_client.set(self.last_processed_key, str(day))
            print(f"Marked day {day} as processed in Redis")
        except Exception as e:
            print(f"Error writing to Redis: {e}")
    
    def get_available_days(self, base_path):
        """Get list of available day directories"""
        available_days = []
        base_path = Path(base_path)
        
        for day_dir in base_path.iterdir():
            if day_dir.is_dir() and day_dir.name.isdigit():
                day_num = int(day_dir.name)
                # Check if there are CSV files in this directory
                csv_files = list(day_dir.glob("*.csv"))
                if csv_files:
                    available_days.append(day_num)
        
        return sorted(available_days)
    
    def get_unprocessed_days(self, base_path):
        """Get list of unprocessed days"""
        available_days = set(self.get_available_days(base_path))
        processed_days = self.get_processed_days()
        unprocessed = available_days - processed_days
        
        print(f"Available days: {sorted(available_days)}")
        print(f"Processed days: {sorted(processed_days)}")
        print(f"Unprocessed days: {sorted(unprocessed)}")
        
        return sorted(unprocessed)

def create_spark_session():
    """Create and configure Spark session"""
    return SparkSession.builder \
        .appName("IncrementalOrdersAggregation") \
        .config("spark.sql.adaptive.enabled", "true") \
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
        .getOrCreate()

def get_category_columns(df):
    """Get all category columns (excluding metadata columns)"""
    excluded_columns = ['order_id', 'order_dow', 'order_hour_of_day', 'days_since_prior_order']
    return [col for col in df.columns if col not in excluded_columns]

def process_day_data(spark, day, base_path, output_path):
    """Process data for a specific day and return aggregated DataFrame"""
    day_path = f"{base_path}/{day}/orders_{day}.csv"
    
    print(f"Processing day {day} from {day_path}")
    
    # Read CSV file for the specific day
    df = spark.read.option("header", "true").option("inferSchema", "true").csv(day_path)
    
    if df.count() == 0:
        print(f"No data found for day {day}")
        return None
    
    print(f"Records for day {day}: {df.count()}")
    
    # Get category columns
    category_columns = get_category_columns(df)
    
    # Create aggregated data for each category
    aggregated_data = []
    
    for category in category_columns:
        # Select relevant columns and filter non-zero values
        category_df = df.select(
            col("order_dow").alias("day_of_week"),
            col("order_hour_of_day").alias("hour_of_day"),
            col(category).alias("items_count")
        ).filter(col(category) > 0)
        
        if category_df.count() == 0:
            continue
            
        # Add category name as a column
        category_df = category_df.withColumn("category", lit(category))
        
        # Group by day_of_week, hour_of_day, category and sum items
        category_aggregated = category_df.groupBy("day_of_week", "hour_of_day", "category") \
            .agg(spark_sum("items_count").alias("items_count"))
        
        aggregated_data.append(category_aggregated)
    
    # Union all category dataframes for this day
    if aggregated_data:
        day_df = aggregated_data[0]
        for df_part in aggregated_data[1:]:
            day_df = day_df.union(df_part)
        
        return day_df
    
    return None

def main():
    """Main execution function"""
    # Initialize components
    processor = IncrementalProcessor()
    spark = create_spark_session()
    
    try:
        # Set paths
        workspace_root = "/workspace" if os.path.exists("/workspace") else "."
        base_path = f"{workspace_root}/data/incremental/raw"
        output_path = f"{workspace_root}/data/processed/incremental_orders"
        
        # Get unprocessed days
        unprocessed_days = processor.get_unprocessed_days(base_path)
        
        if not unprocessed_days:
            print("No new days to process")
            return
        
        print(f"Processing {len(unprocessed_days)} unprocessed days: {unprocessed_days}")
        
        # Process each unprocessed day
        all_processed_data = []
        
        for day in unprocessed_days:
            day_data = process_day_data(spark, day, base_path, output_path)
            
            if day_data is not None:
                all_processed_data.append(day_data)
                
                # Mark day as processed
                processor.mark_day_processed(day)
                print(f"Successfully processed day {day}")
            else:
                print(f"No valid data for day {day}")
        
        # Combine all processed data and append to output
        if all_processed_data:
            # Union all day dataframes
            combined_df = all_processed_data[0]
            for df_part in all_processed_data[1:]:
                combined_df = combined_df.union(df_part)
            
            # Sort results
            combined_df = combined_df.orderBy("day_of_week", "hour_of_day", "category")
            
            print(f"Total new aggregated records: {combined_df.count()}")
            
            # Check if output directory exists and has data
            output_exists = False
            try:
                existing_df = spark.read.option("header", "true").csv(f"{output_path}/*.csv")
                if existing_df.count() > 0:
                    print("Existing data found, appending new results")
                    combined_df = existing_df.union(combined_df) \
                        .groupBy("day_of_week", "hour_of_day", "category") \
                        .agg(spark_sum("items_count").alias("items_count")) \
                        .orderBy("day_of_week", "hour_of_day", "category")
                    output_exists = True
            except:
                print("No existing data found, creating new output")
            
            # Write results
            write_mode = "overwrite"  # Always overwrite to maintain consistency
            combined_df.coalesce(1).write.mode(write_mode).option("header", "true").csv(output_path)
            
            print(f"Results {'appended to' if output_exists else 'written to'}: {output_path}")
            print("Incremental processing completed successfully!")
        
    except Exception as e:
        print(f"Error during incremental processing: {str(e)}")
        import traceback
        traceback.print_exc()
        sys.exit(1)
    finally:
        spark.stop()

if __name__ == "__main__":
    main()