import pandas as pd
import h3
import argparse
import os

def get_hex_safe(lat, lng, res):
    """Converts lat/lng to H3 index."""
    try:
        if hasattr(h3, 'latlng_to_cell'):
            return h3.latlng_to_cell(lat, lng, res)
        return h3.geo_to_h3(lat, lng, res)
    except:
        return None

def main():
    parser = argparse.ArgumentParser(description='Test ride counts at Hex Resolution 8.')
    parser.add_argument('input_csv', nargs='?', default='big-data.csv', help='Original ride data')
    parser.add_argument('output_csv', nargs='?', default='hex8_route_counts.csv', help='Output summary')
    
    args = parser.parse_args()
    
    if not os.path.exists(args.input_csv):
        print(f"Error: {args.input_csv} not found.")
        return

    print(f"Reading {args.input_csv}...")
    df = pd.read_csv(args.input_csv)

    # Use Resolution 8 for testing
    RES = 8

    print(f"Processing coordinates into Hex-{RES}...")
    
    # Calculate hexes on the fly
    df['p_hex8'] = [get_hex_safe(lat, lng, RES) for lat, lng in zip(df['estimated_pickup_latitude'], df['estimated_pickup_longitude'])]
    df['d_hex8'] = [get_hex_safe(lat, lng, RES) for lat, lng in zip(df['estimated_dropoff_latitude'], df['estimated_dropoff_longitude'])]

    print("Aggregating counts...")
    # Group and count
    route_counts = df.groupby(['p_hex8', 'd_hex8']).size().reset_index(name='ride_count')
    
    # Sort by busiest routes
    route_counts = route_counts.sort_values(by='ride_count', ascending=False)

    print(f"Hex-8 Test Results: Found {len(route_counts)} unique routes.")
    print(f"Busiest Hex-8 route has {route_counts['ride_count'].max()} rides.")

    route_counts.to_csv(args.output_csv, index=False)
    print(f"Saved test results to {args.output_csv}")

if __name__ == "__main__":
    main()