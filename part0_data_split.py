#!/usr/bin/env python3
"""
Part 0 - Environment Setup: Data Splitting Script
Split orders.csv by order_dow into 7 separate files
"""

import pandas as pd
import os
from pathlib import Path

def split_orders_by_dow():
    """
    Split the orders.csv file by order_dow (day of week) into separate files
    Creates: part1/data/raw/{dow}/orders_{dow}.csv for each day (0-6)
    """
    # Read the main orders file
    input_file = "part1/data/orders.csv"
    base_output_dir = "part1/data/raw"
    
    print(f"Reading data from {input_file}...")
    df = pd.read_csv(input_file)
    
    print(f"Total records: {len(df)}")
    print(f"Unique days of week: {sorted(df['order_dow'].unique())}")
    
    # Split data by day of week
    for dow in range(7):  # 0-6 representing Sunday to Saturday
        dow_data = df[df['order_dow'] == dow]
        
        if len(dow_data) > 0:
            output_dir = Path(base_output_dir) / str(dow)
            output_dir.mkdir(parents=True, exist_ok=True)
            
            output_file = output_dir / f"orders_{dow}.csv"
            dow_data.to_csv(output_file, index=False)
            
            print(f"Day {dow}: {len(dow_data)} records -> {output_file}")
        else:
            print(f"Day {dow}: No records found")
    
    print("Data splitting completed successfully!")

if __name__ == "__main__":
    # Change to project directory
    os.chdir(Path(__file__).parent)
    split_orders_by_dow()