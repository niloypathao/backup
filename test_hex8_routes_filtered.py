import pandas as pd
import h3
import argparse
import os

def get_hex_safe(lat, lng, res):
    """Converts lat/lng to H3 index safely."""
    try:
        if hasattr(h3, 'latlng_to_cell'):
            return h3.latlng_to_cell(lat, lng, res)
        return h3.geo_to_h3(lat, lng, res)
    except:
        return None

def main():
    parser = argparse.ArgumentParser(description='Aggregate Hex-8 routes and filter low-volume paths.')
    parser.add_argument('input_csv', nargs='?', default='big-data.csv', help='Original ride data')
    parser.add_argument('output_csv', nargs='?', default='hex8_route_counts_filtered.csv', help='Output summary')
    parser.add_argument('--min_rides', type=int, default=5, help='Minimum number of rides to keep a route')
    
    args = parser.parse_args()
    
    if not os.path.exists(args.input_csv):
        print(f"Error: {args.input_csv} not found.")
        return

    print(f"Reading {args.input_csv}...")
    df = pd.read_csv(args.input_csv)

    RES = 8
    print(f"Processing coordinates into Hex-{RES}...")
    
    # Calculate hexes
    df['p_hex8'] = [get_hex_safe(lat, lng, RES) for lat, lng in zip(df['estimated_pickup_latitude'], df['estimated_pickup_longitude'])]
    df['d_hex8'] = [get_hex_safe(lat, lng, RES) for lat, lng in zip(df['estimated_dropoff_latitude'], df['estimated_dropoff_longitude'])]

    print("Aggregating counts...")
    # Group and count
    route_counts = df.groupby(['p_hex8', 'd_hex8']).size().reset_index(name='ride_count')
    
    # --- FILTER LOGIC ---
    initial_count = len(route_counts)
    route_counts = route_counts[route_counts['ride_count'] >= args.min_rides]
    filtered_count = len(route_counts)
    # --------------------

    # Sort by busiest routes
    route_counts = route_counts.sort_values(by='ride_count', ascending=False)

    print(f"Hex-8 Results:")
    print(f"- Total unique routes found: {initial_count}")
    print(f"- Routes remaining after filtering (>= {args.min_rides} rides): {filtered_count}")
    print(f"- Reduction: {initial_count - filtered_count} low-volume routes removed.")

    route_counts.to_csv(args.output_csv, index=False)
    print(f"Saved filtered results to {args.output_csv}")

if __name__ == "__main__":
    main()