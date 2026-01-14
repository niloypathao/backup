import pandas as pd
import argparse
import os

def main():
    parser = argparse.ArgumentParser(description='Aggregate ride counts for hex-to-hex routes.')
    parser.add_argument('input_csv', nargs='?', default='big-data-with-hex.csv', help='Input enriched CSV')
    parser.add_argument('output_csv', nargs='?', default='hex_route_counts.csv', help='Output summary CSV')
    
    args = parser.parse_args()
    
    if not os.path.exists(args.input_csv):
        print(f"Error: {args.input_csv} not found. Please run the previous script first.")
        return

    print(f"Reading {args.input_csv}...")
    df = pd.read_csv(args.input_csv)

    # 1. Group by the pickup and dropoff hex pair
    print("Grouping by routes and counting rides...")
    
    # We group by both hex columns and count the 'hashed_id' (or any column)
    route_counts = df.groupby(['pickup_hex_9', 'dropoff_hex_9']).size().reset_index(name='ride_count')

    # 2. Sort by highest ride count so the busiest routes are at the top
    route_counts = route_counts.sort_values(by='ride_count', ascending=False)

    print(f"Found {len(route_counts)} unique hex-to-hex routes.")
    
    # 3. Save to CSV
    route_counts.to_csv(args.output_csv, index=False)
    print(f"Successfully saved route counts to {args.output_csv}")

if __name__ == "__main__":
    main()