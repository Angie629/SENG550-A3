#!/usr/bin/env python3
"""
Part 4 - Cached Inference Script
Fast inference using Redis cache instead of model prediction
"""

import redis
import json
import sys
from datetime import datetime

def connect_redis(host="redis", port=6379, db=1):
    """Connect to Redis for reading cached predictions"""
    try:
        r = redis.Redis(host=host, port=port, db=db, decode_responses=True)
        r.ping()
        print(f"Redis connection successful: {host}:{port}")
        return r
    except Exception as e:
        print(f"Redis connection failed: {e}")
        return None

def get_cached_prediction(redis_client, day_of_week, hour_of_day, category):
    """
    Get prediction from Redis cache
    
    Args:
        redis_client: Redis connection
        day_of_week: int (0-6)
        hour_of_day: int (0-23) 
        category: string
    
    Returns:
        dict: prediction data or None if not found
    """
    # Create Redis key
    redis_key = f"{day_of_week}:{hour_of_day}:{category.lower().strip()}"
    
    try:
        cached_data = redis_client.get(redis_key)
        
        if cached_data:
            prediction_data = json.loads(cached_data)
            return prediction_data
        else:
            return None
            
    except Exception as e:
        print(f"Error retrieving cached prediction: {e}")
        return None

def get_batch_cached_predictions(redis_client, requests):
    """
    Get multiple predictions from cache
    
    Args:
        redis_client: Redis connection
        requests: List of tuples (day_of_week, hour_of_day, category)
    
    Returns:
        List of prediction results
    """
    results = []
    
    for day_of_week, hour_of_day, category in requests:
        prediction_data = get_cached_prediction(redis_client, day_of_week, hour_of_day, category)
        
        if prediction_data:
            results.append({
                'day_of_week': day_of_week,
                'hour_of_day': hour_of_day,
                'category': category,
                'prediction': prediction_data['prediction'],
                'cached_at': prediction_data.get('cached_at', 'unknown'),
                'found': True
            })
        else:
            results.append({
                'day_of_week': day_of_week,
                'hour_of_day': hour_of_day,
                'category': category,
                'prediction': None,
                'found': False
            })
    
    return results

def get_cache_statistics(redis_client):
    """Get statistics about the cache"""
    try:
        # Get metadata
        metadata = redis_client.get("cache_metadata")
        if metadata:
            metadata_obj = json.loads(metadata)
        else:
            metadata_obj = {}
        
        # Count prediction keys
        prediction_keys = redis_client.keys("*:*:*")  # Pattern: day:hour:category
        
        # Get sample of categories
        categories = set()
        for key in prediction_keys[:100]:  # Sample first 100 keys
            parts = key.split(":")
            if len(parts) == 3:
                categories.add(parts[2])
        
        stats = {
            'total_cached_predictions': len(prediction_keys),
            'cache_metadata': metadata_obj,
            'sample_categories': sorted(list(categories)),
            'cache_last_updated': metadata_obj.get('last_updated', 'unknown')
        }
        
        return stats
        
    except Exception as e:
        print(f"Error getting cache statistics: {e}")
        return {}

def find_top_predictions(redis_client, day_of_week=None, hour_of_day=None):
    """Find top predictions for a specific day/hour or overall"""
    try:
        # Build pattern based on filters
        if day_of_week is not None and hour_of_day is not None:
            pattern = f"{day_of_week}:{hour_of_day}:*"
        elif day_of_week is not None:
            pattern = f"{day_of_week}:*:*"
        elif hour_of_day is not None:
            pattern = f"*:{hour_of_day}:*"
        else:
            pattern = "*:*:*"
        
        keys = redis_client.keys(pattern)
        
        # Get predictions
        predictions = []
        for key in keys[:1000]:  # Limit to first 1000 for performance
            data = redis_client.get(key)
            if data:
                prediction_obj = json.loads(data)
                predictions.append({
                    'key': key,
                    'prediction': prediction_obj['prediction'],
                    'day_of_week': prediction_obj['day_of_week'],
                    'hour_of_day': prediction_obj['hour_of_day'],
                    'category': prediction_obj['category']
                })
        
        # Sort by prediction value
        predictions.sort(key=lambda x: x['prediction'], reverse=True)
        
        return predictions[:20]  # Return top 20
        
    except Exception as e:
        print(f"Error finding top predictions: {e}")
        return []

def main():
    """Main cached inference function"""
    # Connect to Redis
    redis_client = connect_redis()
    if redis_client is None:
        print("Cannot connect to Redis, exiting...")
        sys.exit(1)
    
    try:
        if len(sys.argv) == 4:
            # Single prediction lookup
            day_of_week = int(sys.argv[1])
            hour_of_day = int(sys.argv[2]) 
            category = sys.argv[3]
            
            print(f"Looking up cached prediction for: day={day_of_week}, hour={hour_of_day}, category={category}")
            
            prediction_data = get_cached_prediction(redis_client, day_of_week, hour_of_day, category)
            
            if prediction_data:
                print(f"Cached prediction: {prediction_data['prediction']} items")
                print(f"Cached at: {prediction_data.get('cached_at', 'unknown')}")
            else:
                print("Prediction not found in cache")
        
        elif len(sys.argv) == 2:
            if sys.argv[1] == "stats":
                # Show cache statistics
                print("=== CACHE STATISTICS ===")
                stats = get_cache_statistics(redis_client)
                
                print(f"Total cached predictions: {stats.get('total_cached_predictions', 0)}")
                print(f"Last updated: {stats.get('cache_last_updated', 'unknown')}")
                
                if stats.get('sample_categories'):
                    print(f"Sample categories: {stats['sample_categories'][:10]}")
                
                metadata = stats.get('cache_metadata', {})
                if metadata:
                    print("Cache metadata:")
                    for key, value in metadata.items():
                        print(f"  {key}: {value}")
            
            elif sys.argv[1] == "top":
                # Show top predictions overall
                print("=== TOP PREDICTIONS (OVERALL) ===")
                top_predictions = find_top_predictions(redis_client)
                
                for i, pred in enumerate(top_predictions[:10], 1):
                    print(f"{i:2d}. Day {pred['day_of_week']}, Hour {pred['hour_of_day']:2d}, "
                          f"{pred['category']:20s}: {pred['prediction']:6.1f} items")
            
            elif sys.argv[1].startswith("day"):
                # Top predictions for a specific day
                day = int(sys.argv[1].replace("day", ""))
                print(f"=== TOP PREDICTIONS FOR DAY {day} ===")
                top_predictions = find_top_predictions(redis_client, day_of_week=day)
                
                for i, pred in enumerate(top_predictions[:10], 1):
                    print(f"{i:2d}. Hour {pred['hour_of_day']:2d}, "
                          f"{pred['category']:20s}: {pred['prediction']:6.1f} items")
        
        else:
            # Demo mode - show various cached lookups
            print("=== CACHED INFERENCE DEMO ===")
            
            # Example requests
            demo_requests = [
                (0, 9, "coffee"),       # Sunday morning coffee
                (1, 18, "fresh_fruits"), # Monday evening fruits
                (2, 12, "milk"),        # Tuesday noon milk
                (3, 8, "bread"),        # Wednesday morning bread
                (4, 19, "frozen_meals"), # Thursday evening frozen meals
                (5, 7, "cereal"),       # Friday morning cereal
                (6, 20, "snacks"),      # Saturday evening snacks
            ]
            
            day_names = ["Sunday", "Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday"]
            
            print("Sample cached predictions:")
            for day, hour, category in demo_requests:
                prediction_data = get_cached_prediction(redis_client, day, hour, category)
                
                if prediction_data:
                    print(f"{day_names[day]} {hour:2d}:00, {category:15s}: {prediction_data['prediction']:6.1f} items")
                else:
                    print(f"{day_names[day]} {hour:2d}:00, {category:15s}: NOT CACHED")
            
            # Show cache performance info
            print("\n=== CACHE PERFORMANCE ===")
            stats = get_cache_statistics(redis_client)
            print(f"Total cached predictions: {stats.get('total_cached_predictions', 0)}")
            print("Cache lookup is near-instant (< 1ms)")
            print("No model loading or Spark computation required")
        
        print("\n=== CACHED INFERENCE COMPLETED ===")
        
    except Exception as e:
        print(f"Error in cached inference: {str(e)}")
        import traceback
        traceback.print_exc()
        sys.exit(1)

if __name__ == "__main__":
    main()