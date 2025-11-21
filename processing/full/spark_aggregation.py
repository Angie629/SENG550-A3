#!/usr/bin/env python3
"""
Part 1 - Data Aggregation with Spark
Aggregate number of items sold per category, grouped by day_of_week, hour_of_day, category
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum as spark_sum, lit
from pyspark.sql.types import *
import sys
import os

def create_spark_session():
    """Create and configure Spark session"""
    return SparkSession.builder \
        .appName("OrdersDataAggregation") \
        .config("spark.sql.adaptive.enabled", "true") \
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
        .getOrCreate()

def get_category_columns(df):
    """Get all category columns (excluding metadata columns)"""
    excluded_columns = ['order_id', 'order_dow', 'order_hour_of_day', 'days_since_prior_order']
    return [col for col in df.columns if col not in excluded_columns]

def aggregate_orders_data(spark, input_path_pattern, output_path):
    """
    Aggregate orders data by day_of_week, hour_of_day, and category
    
    Args:
        spark: SparkSession
        input_path_pattern: Path pattern to read raw data files
        output_path: Path to save aggregated results
    """
    print(f"Reading data from: {input_path_pattern}")
    
    # Read all CSV files from raw data directories
    df = spark.read.option("header", "true").option("inferSchema", "true").csv(input_path_pattern)
    
    print(f"Total records loaded: {df.count()}")
    print("Schema:")
    df.printSchema()
    
    # Get category columns
    category_columns = get_category_columns(df)
    print(f"Found {len(category_columns)} product categories")
    
    # Create aggregated data for each category
    aggregated_data = []
    
    for category in category_columns:
        print(f"Processing category: {category}")
        
        # Select relevant columns and filter non-zero values
        category_df = df.select(
            col("order_dow").alias("day_of_week"),
            col("order_hour_of_day").alias("hour_of_day"),
            col(category).alias("items_count")
        ).filter(col(category) > 0)
        
        # Add category name as a column
        category_df = category_df.withColumn("category", lit(category))
        
        # Group by day_of_week, hour_of_day, category and sum items
        category_aggregated = category_df.groupBy("day_of_week", "hour_of_day", "category") \
            .agg(spark_sum("items_count").alias("items_count"))
        
        aggregated_data.append(category_aggregated)
    
    # Union all category dataframes
    if aggregated_data:
        final_df = aggregated_data[0]
        for df_part in aggregated_data[1:]:
            final_df = final_df.union(df_part)
        
        # Sort by day_of_week, hour_of_day, category for better readability
        final_df = final_df.orderBy("day_of_week", "hour_of_day", "category")
        
        print(f"Final aggregated records: {final_df.count()}")
        
        # Show sample of results
        print("Sample aggregated data:")
        final_df.show(20, truncate=False)
        
        # Write results to CSV
        print(f"Saving results to: {output_path}")
        final_df.coalesce(1).write.mode("overwrite").option("header", "true").csv(output_path)
        
        print("Aggregation completed successfully!")
        return final_df
    else:
        print("No data to process!")
        return None

def main():
    """Main execution function"""
    # Create Spark session
    spark = create_spark_session()
    
    try:
        # Set paths
        workspace_root = "/workspace" if os.path.exists("/workspace") else "."
        input_path = f"{workspace_root}/part1/data/raw/*/orders_*.csv"
        output_path = f"{workspace_root}/data/processed/orders"
        
        # Run aggregation
        result_df = aggregate_orders_data(spark, input_path, output_path)
        
        if result_df:
            print("\n=== AGGREGATION SUMMARY ===")
            print(f"Total unique combinations: {result_df.count()}")
            print(f"Categories processed: {result_df.select('category').distinct().count()}")
            print(f"Days of week: {result_df.select('day_of_week').distinct().count()}")
            print(f"Hours of day: {result_df.select('hour_of_day').distinct().count()}")
            
    except Exception as e:
        print(f"Error during processing: {str(e)}")
        sys.exit(1)
    finally:
        spark.stop()

if __name__ == "__main__":
    main()