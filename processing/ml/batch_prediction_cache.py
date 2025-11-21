#!/usr/bin/env python3
"""
Part 4 - Batch Prediction Pipeline with Redis Caching
Generate predictions for all combinations and store in Redis for fast lookups
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit, concat_ws
from pyspark.sql.types import *
from pyspark.ml import PipelineModel
import redis
import json
import sys
import os
from datetime import datetime

def create_spark_session():
    """Create and configure Spark session"""
    return SparkSession.builder \
        .appName("BatchPredictionCaching") \
        .config("spark.sql.adaptive.enabled", "true") \
        .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer") \
        .getOrCreate()

def connect_redis(host="redis", port=6379, db=1):
    """Connect to Redis for caching predictions"""
    try:
        r = redis.Redis(host=host, port=port, db=db, decode_responses=True)
        r.ping()
        print(f"Redis connection successful: {host}:{port}")
        return r
    except Exception as e:
        print(f"Redis connection failed: {e}")
        return None

def load_model(model_path):
    """Load the trained ML model"""
    try:
        model = PipelineModel.load(model_path)
        print(f"Model loaded successfully from: {model_path}")
        return model
    except Exception as e:
        print(f"Error loading model: {e}")
        return None

def generate_all_combinations(spark):
    """Generate all possible combinations of day_of_week, hour_of_day, category"""
    print("Generating all possible combinations for prediction...")
    
    # Comprehensive list of categories (should match training data)
    categories = [
        "fresh_fruits", "fresh_vegetables", "milk", "bread", "eggs",
        "coffee", "cereal", "yogurt", "cheese", "chicken",
        "beef", "pasta", "rice", "beans", "juice",
        "frozen_meals", "snacks", "beverages", "cleaning_products", "personal_care",
        "bakery_desserts", "canned_goods", "dairy_products", "meat_seafood",
        "organic_products", "baby_food", "pet_care", "vitamins_supplements",
        "ice_cream", "candy_chocolate", "tea", "soft_drinks"
    ]
    
    # Generate all combinations
    combinations = []
    for dow in range(7):  # days 0-6 (Sunday to Saturday)
        for hour in range(24):  # hours 0-23
            for category in categories:
                combinations.append((dow, hour, category))
    
    # Create DataFrame
    schema = StructType([
        StructField("day_of_week", IntegerType(), True),
        StructField("hour_of_day", IntegerType(), True),
        StructField("category_clean", StringType(), True)
    ])
    
    combinations_df = spark.createDataFrame(combinations, schema)
    print(f"Generated {combinations_df.count()} combinations")
    
    return combinations_df

def batch_predict_and_cache(spark, model, redis_client):
    """Generate predictions for all combinations and cache in Redis"""
    print("Starting batch prediction and caching process...")
    
    # Generate all combinations
    combinations_df = generate_all_combinations(spark)
    
    # Make batch predictions
    try:
        print("Making batch predictions...")
        predictions_df = model.transform(combinations_df)
        
        # Add Redis key column
        predictions_with_key = predictions_df.withColumn(
            "redis_key",
            concat_ws(":", col("day_of_week"), col("hour_of_day"), col("category_clean"))
        )
        
        # Collect results (might be memory intensive for large datasets)
        print("Collecting predictions...")
        results = predictions_with_key.select(
            "redis_key", "day_of_week", "hour_of_day", "category_clean", "prediction"
        ).collect()
        
        print(f"Caching {len(results)} predictions to Redis...")
        
        # Cache predictions in Redis
        pipeline = redis_client.pipeline()
        batch_size = 1000
        cached_count = 0
        
        for i, row in enumerate(results):
            redis_key = row.redis_key
            prediction_value = round(row.prediction, 2)
            
            # Store prediction with metadata
            prediction_data = {
                "prediction": prediction_value,
                "day_of_week": row.day_of_week,
                "hour_of_day": row.hour_of_day,
                "category": row.category_clean,
                "cached_at": datetime.now().isoformat()
            }
            
            pipeline.set(redis_key, json.dumps(prediction_data))
            
            # Execute pipeline in batches
            if (i + 1) % batch_size == 0:
                pipeline.execute()
                cached_count += batch_size
                print(f"Cached {cached_count}/{len(results)} predictions...")
                pipeline = redis_client.pipeline()
        
        # Execute remaining items
        if len(results) % batch_size != 0:
            pipeline.execute()
            cached_count = len(results)
        
        # Set metadata about the cache
        cache_metadata = {
            "total_predictions": len(results),
            "last_updated": datetime.now().isoformat(),
            "model_version": "v1.0",
            "categories_count": len(set(row.category_clean for row in results)),
            "days_range": "0-6",
            "hours_range": "0-23"
        }
        
        redis_client.set("cache_metadata", json.dumps(cache_metadata))
        
        print(f"Successfully cached {cached_count} predictions!")
        return cached_count
        
    except Exception as e:
        print(f"Error in batch prediction and caching: {e}")
        import traceback
        traceback.print_exc()
        return 0

def validate_cache(redis_client):
    """Validate that predictions are properly cached"""
    try:
        # Check metadata
        metadata = redis_client.get("cache_metadata")
        if metadata:
            metadata_obj = json.loads(metadata)
            print("Cache Metadata:")
            for key, value in metadata_obj.items():
                print(f"  {key}: {value}")
        
        # Test some sample lookups
        sample_keys = [
            "0:9:coffee",      # Sunday morning coffee
            "1:18:fresh_fruits", # Monday evening fruits
            "5:12:milk",       # Friday noon milk
            "6:8:bread"        # Saturday morning bread
        ]
        
        print("\nSample cache lookups:")
        for key in sample_keys:
            value = redis_client.get(key)
            if value:
                prediction_data = json.loads(value)
                print(f"  {key}: {prediction_data['prediction']} items")
            else:
                print(f"  {key}: NOT FOUND")
        
        # Count total cached keys (excluding metadata)
        all_keys = redis_client.keys("*:*:*")  # Pattern: day:hour:category
        print(f"\nTotal cached predictions: {len(all_keys)}")
        
        return len(all_keys) > 0
        
    except Exception as e:
        print(f"Error validating cache: {e}")
        return False

def main():
    """Main batch prediction and caching function"""
    spark = create_spark_session()
    
    try:
        # Set paths
        workspace_root = "/workspace" if os.path.exists("/workspace") else "."
        model_path = f"{workspace_root}/processing/ml/trained_model"
        
        print(f"=== BATCH PREDICTION CACHING STARTED at {datetime.now()} ===")
        
        # Connect to Redis
        redis_client = connect_redis()
        if redis_client is None:
            print("Cannot connect to Redis, exiting...")
            sys.exit(1)
        
        # Load model
        model = load_model(model_path)
        if model is None:
            print("Cannot load model, exiting...")
            sys.exit(1)
        
        # Clear existing cache (optional - for fresh start)
        if len(sys.argv) > 1 and sys.argv[1] == "clear":
            print("Clearing existing cache...")
            redis_client.flushdb()
        
        # Generate and cache predictions
        cached_count = batch_predict_and_cache(spark, model, redis_client)
        
        if cached_count > 0:
            print(f"Successfully cached {cached_count} predictions")
            
            # Validate cache
            if validate_cache(redis_client):
                print("Cache validation successful")
            else:
                print("Cache validation failed")
            
            print("=== BATCH PREDICTION CACHING COMPLETED ===")
        else:
            print("=== BATCH PREDICTION CACHING FAILED ===")
            sys.exit(1)
            
    except Exception as e:
        print(f"Error in batch prediction caching: {str(e)}")
        import traceback
        traceback.print_exc()
        sys.exit(1)
    finally:
        spark.stop()

if __name__ == "__main__":
    main()