#!/usr/bin/env python3
"""
Part 3 - Machine Learning with Spark
Train a Spark ML model to predict number of orders per category per day/hour
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, regexp_replace, lower, trim
from pyspark.sql.types import *
from pyspark.ml.feature import StringIndexer, VectorAssembler, OneHotEncoder
from pyspark.ml.regression import RandomForestRegressor, LinearRegression, DecisionTreeRegressor
from pyspark.ml import Pipeline
from pyspark.ml.evaluation import RegressionEvaluator
import os
import sys
from datetime import datetime

def create_spark_session():
    """Create and configure Spark session"""
    return SparkSession.builder \
        .appName("OrdersMLTraining") \
        .config("spark.sql.adaptive.enabled", "true") \
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
        .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer") \
        .getOrCreate()

def prepare_training_data(spark, data_path):
    """
    Load and prepare training data from aggregated results
    
    Returns:
        DataFrame with features: day_of_week, hour_of_day, category_encoded
        Target: items_count (renamed to total_count)
    """
    print(f"Loading training data from: {data_path}")
    
    try:
        # Try to read from processed data (either from Part 1 or Part 2)
        df = spark.read.option("header", "true").option("inferSchema", "true").csv(f"{data_path}/*.csv")
        
        if df.count() == 0:
            print("No data found in processed directory, trying incremental directory")
            incremental_path = data_path.replace("/processed/", "/processed/incremental_")
            df = spark.read.option("header", "true").option("inferSchema", "true").csv(f"{incremental_path}/*.csv")
    
    except Exception as e:
        print(f"Error reading processed data: {e}")
        print("Falling back to generating sample data...")
        return generate_sample_data(spark)
    
    if df.count() == 0:
        print("No aggregated data available, generating sample data...")
        return generate_sample_data(spark)
    
    print(f"Loaded {df.count()} records for training")
    
    # Clean and prepare the data
    df_clean = df.select(
        col("day_of_week").cast(IntegerType()),
        col("hour_of_day").cast(IntegerType()),
        regexp_replace(trim(lower(col("category"))), "[^a-z0-9 ]", "").alias("category_clean"),
        col("items_count").cast(DoubleType()).alias("total_count")
    ).filter(
        (col("day_of_week").isNotNull()) &
        (col("hour_of_day").isNotNull()) &
        (col("category_clean").isNotNull()) &
        (col("total_count").isNotNull()) &
        (col("total_count") > 0)
    )
    
    print("Sample of cleaned data:")
    df_clean.show(10, truncate=False)
    
    return df_clean

def generate_sample_data(spark):
    """Generate sample training data if no processed data is available"""
    print("Generating sample training data...")
    
    import random
    from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DoubleType
    
    # Sample categories
    categories = [
        "fresh_fruits", "fresh_vegetables", "milk", "bread", "eggs",
        "coffee", "cereal", "yogurt", "cheese", "chicken",
        "beef", "pasta", "rice", "beans", "juice"
    ]
    
    # Generate sample data
    sample_data = []
    for dow in range(7):  # days 0-6
        for hour in range(24):  # hours 0-23
            for category in categories:
                # Generate realistic item counts with some patterns
                base_count = random.randint(1, 50)
                # Weekend effect
                if dow in [0, 6]:
                    base_count = int(base_count * 1.5)
                # Peak hours effect (morning and evening)
                if hour in [7, 8, 9, 17, 18, 19]:
                    base_count = int(base_count * 1.8)
                # Late night/early morning reduction
                if hour < 6 or hour > 22:
                    base_count = int(base_count * 0.3)
                
                sample_data.append((dow, hour, category, float(base_count)))
    
    # Create DataFrame
    schema = StructType([
        StructField("day_of_week", IntegerType(), True),
        StructField("hour_of_day", IntegerType(), True),
        StructField("category_clean", StringType(), True),
        StructField("total_count", DoubleType(), True)
    ])
    
    df = spark.createDataFrame(sample_data, schema)
    print(f"Generated {df.count()} sample records")
    return df

def train_model(spark, training_data, model_output_path):
    """
    Train multiple regression models and save the best one
    
    Args:
        spark: SparkSession
        training_data: DataFrame with features and target
        model_output_path: Path to save the trained model
    """
    print("Preparing features for ML training...")
    
    # String indexer for category
    category_indexer = StringIndexer(
        inputCol="category_clean",
        outputCol="category_indexed",
        handleInvalid="keep"
    )
    
    # One-hot encoder for category
    category_encoder = OneHotEncoder(
        inputCol="category_indexed",
        outputCol="category_encoded"
    )
    
    # Assemble all features
    feature_assembler = VectorAssembler(
        inputCols=["day_of_week", "hour_of_day", "category_encoded"],
        outputCol="features"
    )
    
    # Split data for training and validation
    train_data, test_data = training_data.randomSplit([0.8, 0.2], seed=42)
    
    print(f"Training set: {train_data.count()} records")
    print(f"Test set: {test_data.count()} records")
    
    # Define models to try
    models = {
        "RandomForest": RandomForestRegressor(
            featuresCol="features",
            labelCol="total_count",
            numTrees=20,
            maxDepth=10,
            seed=42
        ),
        "LinearRegression": LinearRegression(
            featuresCol="features",
            labelCol="total_count",
            regParam=0.1
        ),
        "DecisionTree": DecisionTreeRegressor(
            featuresCol="features",
            labelCol="total_count",
            maxDepth=10,
            seed=42
        )
    }
    
    best_model = None
    best_rmse = float('inf')
    best_name = None
    
    evaluator = RegressionEvaluator(
        labelCol="total_count",
        predictionCol="prediction",
        metricName="rmse"
    )
    
    # Train and evaluate each model
    for model_name, model in models.items():
        print(f"\nTraining {model_name}...")
        
        # Create pipeline
        pipeline = Pipeline(stages=[
            category_indexer,
            category_encoder,
            feature_assembler,
            model
        ])
        
        try:
            # Train model
            fitted_pipeline = pipeline.fit(train_data)
            
            # Make predictions
            predictions = fitted_pipeline.transform(test_data)
            
            # Evaluate
            rmse = evaluator.evaluate(predictions)
            print(f"{model_name} RMSE: {rmse:.4f}")
            
            # Keep track of best model
            if rmse < best_rmse:
                best_rmse = rmse
                best_model = fitted_pipeline
                best_name = model_name
                
        except Exception as e:
            print(f"Error training {model_name}: {e}")
    
    if best_model is not None:
        print(f"\nBest model: {best_name} with RMSE: {best_rmse:.4f}")
        
        # Save the best model
        try:
            best_model.write().overwrite().save(model_output_path)
            print(f"Model saved to: {model_output_path}")
            
            # Show sample predictions
            sample_predictions = best_model.transform(test_data.limit(10))
            print("\nSample predictions:")
            sample_predictions.select("day_of_week", "hour_of_day", "category_clean", 
                                    "total_count", "prediction").show(truncate=False)
            
            return best_model
        except Exception as e:
            print(f"Error saving model: {e}")
            return None
    else:
        print("No successful model training!")
        return None

def main():
    """Main training function"""
    spark = create_spark_session()
    
    try:
        # Set paths
        workspace_root = "/workspace" if os.path.exists("/workspace") else "."
        data_path = f"{workspace_root}/data/processed/orders"
        model_output_path = f"{workspace_root}/processing/ml/trained_model"
        
        print(f"=== ML TRAINING STARTED at {datetime.now()} ===")
        
        # Prepare training data
        training_data = prepare_training_data(spark, data_path)
        
        if training_data is None or training_data.count() == 0:
            print("No training data available!")
            sys.exit(1)
        
        # Train model
        trained_model = train_model(spark, training_data, model_output_path)
        
        if trained_model is not None:
            print("=== ML TRAINING COMPLETED SUCCESSFULLY ===")
            
            # Log training summary
            total_records = training_data.count()
            unique_categories = training_data.select("category_clean").distinct().count()
            unique_hours = training_data.select("hour_of_day").distinct().count()
            unique_days = training_data.select("day_of_week").distinct().count()
            
            print(f"Training summary:")
            print(f"- Total records: {total_records}")
            print(f"- Unique categories: {unique_categories}")
            print(f"- Hours range: {unique_hours}")
            print(f"- Days range: {unique_days}")
            
        else:
            print("=== ML TRAINING FAILED ===")
            sys.exit(1)
            
    except Exception as e:
        print(f"Error in ML training: {str(e)}")
        import traceback
        traceback.print_exc()
        sys.exit(1)
    finally:
        spark.stop()

if __name__ == "__main__":
    main()