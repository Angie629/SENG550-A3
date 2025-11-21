#!/usr/bin/env python3
"""
Part 3 - ML Inference Script
Load trained model and make predictions for given inputs
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit
from pyspark.sql.types import *
from pyspark.ml import PipelineModel
import os
import sys

def create_spark_session():
    """Create and configure Spark session"""
    return SparkSession.builder \
        .appName("OrdersMLInference") \
        .config("spark.sql.adaptive.enabled", "true") \
        .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer") \
        .getOrCreate()

def load_model(model_path):
    """Load the trained ML model"""
    try:
        model = PipelineModel.load(model_path)
        print(f"Model loaded successfully from: {model_path}")
        return model
    except Exception as e:
        print(f"Error loading model: {e}")
        return None

def predict_single(model, spark, day_of_week, hour_of_day, category):
    """
    Make prediction for a single input
    
    Args:
        model: Trained PipelineModel
        spark: SparkSession
        day_of_week: int (0-6)
        hour_of_day: int (0-23)
        category: string
    
    Returns:
        float: predicted item count
    """
    # Create input DataFrame
    input_data = [(day_of_week, hour_of_day, category.lower().strip())]
    schema = StructType([
        StructField("day_of_week", IntegerType(), True),
        StructField("hour_of_day", IntegerType(), True),
        StructField("category_clean", StringType(), True)
    ])
    
    input_df = spark.createDataFrame(input_data, schema)
    
    # Make prediction
    try:
        prediction_df = model.transform(input_df)
        prediction = prediction_df.select("prediction").collect()[0][0]
        return round(prediction, 2)
    except Exception as e:
        print(f"Error making prediction: {e}")
        return None

def predict_batch(model, spark, inputs):
    """
    Make predictions for multiple inputs
    
    Args:
        model: Trained PipelineModel
        spark: SparkSession
        inputs: List of tuples (day_of_week, hour_of_day, category)
    
    Returns:
        List of predictions
    """
    # Create input DataFrame
    input_data = [(int(dow), int(hour), str(cat).lower().strip()) for dow, hour, cat in inputs]
    schema = StructType([
        StructField("day_of_week", IntegerType(), True),
        StructField("hour_of_day", IntegerType(), True),
        StructField("category_clean", StringType(), True)
    ])
    
    input_df = spark.createDataFrame(input_data, schema)
    
    # Make predictions
    try:
        predictions_df = model.transform(input_df)
        results = predictions_df.select("day_of_week", "hour_of_day", "category_clean", "prediction").collect()
        
        predictions = []
        for row in results:
            predictions.append({
                'day_of_week': row.day_of_week,
                'hour_of_day': row.hour_of_day,
                'category': row.category_clean,
                'prediction': round(row.prediction, 2)
            })
        
        return predictions
    except Exception as e:
        print(f"Error making batch predictions: {e}")
        return None

def generate_all_predictions(model, spark):
    """Generate predictions for all possible combinations"""
    print("Generating predictions for all combinations...")
    
    # Sample categories (should match training data)
    categories = [
        "fresh_fruits", "fresh_vegetables", "milk", "bread", "eggs",
        "coffee", "cereal", "yogurt", "cheese", "chicken",
        "beef", "pasta", "rice", "beans", "juice",
        "frozen_meals", "snacks", "beverages", "cleaning_products", "personal_care"
    ]
    
    # Generate all combinations
    all_combinations = []
    for dow in range(7):  # days 0-6
        for hour in range(24):  # hours 0-23
            for category in categories:
                all_combinations.append((dow, hour, category))
    
    print(f"Generating {len(all_combinations)} predictions...")
    
    # Make batch predictions
    predictions = predict_batch(model, spark, all_combinations)
    
    if predictions:
        print(f"Generated {len(predictions)} predictions")
        return predictions
    else:
        return []

def main():
    """Main inference function"""
    spark = create_spark_session()
    
    try:
        # Set paths
        workspace_root = "/workspace" if os.path.exists("/workspace") else "."
        model_path = f"{workspace_root}/processing/ml/trained_model"
        
        # Load model
        model = load_model(model_path)
        if model is None:
            print("Cannot load model, exiting...")
            sys.exit(1)
        
        # Check command line arguments for specific prediction
        if len(sys.argv) == 4:
            # Single prediction mode
            day_of_week = int(sys.argv[1])
            hour_of_day = int(sys.argv[2])
            category = sys.argv[3]
            
            print(f"Making prediction for: day={day_of_week}, hour={hour_of_day}, category={category}")
            
            prediction = predict_single(model, spark, day_of_week, hour_of_day, category)
            
            if prediction is not None:
                print(f"Predicted items: {prediction}")
            else:
                print("Prediction failed")
        
        elif len(sys.argv) == 2 and sys.argv[1] == "all":
            # Generate all predictions mode
            predictions = generate_all_predictions(model, spark)
            
            if predictions:
                print("\nSample predictions:")
                for i, pred in enumerate(predictions[:10]):  # Show first 10
                    print(f"Day {pred['day_of_week']}, Hour {pred['hour_of_day']}, "
                          f"Category: {pred['category']}, Predicted: {pred['prediction']}")
                
                if len(predictions) > 10:
                    print(f"... and {len(predictions) - 10} more predictions")
        
        else:
            # Demo mode - show some example predictions
            print("=== DEMO PREDICTIONS ===")
            
            # Example predictions
            examples = [
                (0, 9, "coffee"),      # Sunday morning coffee
                (1, 18, "fresh_fruits"), # Monday evening fruits
                (5, 12, "milk"),       # Friday noon milk
                (6, 8, "bread"),       # Saturday morning bread
                (3, 20, "frozen_meals") # Wednesday evening frozen meals
            ]
            
            for day, hour, category in examples:
                prediction = predict_single(model, spark, day, hour, category)
                if prediction is not None:
                    print(f"Day {day}, Hour {hour}, {category}: {prediction} items")
                else:
                    print(f"Failed to predict for Day {day}, Hour {hour}, {category}")
        
        print("\n=== INFERENCE COMPLETED ===")
        
    except Exception as e:
        print(f"Error in inference: {str(e)}")
        import traceback
        traceback.print_exc()
        sys.exit(1)
    finally:
        spark.stop()

if __name__ == "__main__":
    main()