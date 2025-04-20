import json
import os
import glob
import pandas as pd
import matplotlib.pyplot as plt
import numpy as np
import argparse
from typing import List, Dict, Any, Tuple, Optional

def load_benchmark_file(file_path: str) -> Tuple[Dict[str, Any], pd.DataFrame]:
    """
    Load a benchmark file and extract the metadata and stats.
    
    Args:
        file_path: Path to the benchmark file
        
    Returns:
        Tuple of (metadata, stats_df)
    """
    with open(file_path, 'r') as f:
        data = json.load(f)
    
    if not isinstance(data, dict) or 'run_metadata' not in data or 'historical_stats' not in data:
        raise ValueError(f"Invalid data format in {file_path}")
    
    metadata = data['run_metadata']
    
    # Extract data from historical_stats
    stats_data = []
    for item in data['historical_stats']:
        if 'stats' in item and 'timestamp' in item:
            timestamp = item['timestamp']
            stats = item['stats']
            
            # Combine all stats into a single row
            row = {'timestamp': timestamp}
            
            # Extract locust data
            if 'locust' in stats:
                for k, v in stats['locust'].items():
                    row[f'locust_{k}'] = v
            
            # Extract container insights data
            if 'container_insights' in stats:
                for k, v in stats['container_insights'].items():
                    row[f'container_{k}'] = v
            
            # Extract volga on-demand data
            if 'volga_on_demand' in stats:
                for k, v in stats['volga_on_demand'].items():
                    row[f'volga_{k}'] = v
            
            stats_data.append(row)
    
    # Convert to DataFrame
    stats_df = pd.DataFrame(stats_data)
    
    # Convert timestamp to datetime
    if 'timestamp' in stats_df.columns:
        stats_df['timestamp'] = pd.to_datetime(stats_df['timestamp'], unit='s')
    
    return metadata, stats_df

def find_max_rps_below_threshold(stats_df: pd.DataFrame, latency_threshold: float) -> Dict[str, Any]:
    """
    Find the maximum RPS where both end-to-end P95 and DB P95 latencies are below the threshold.
    
    Args:
        stats_df: DataFrame containing the stats
        latency_threshold: Latency threshold in ms
        
    Returns:
        Dictionary with the max RPS and corresponding stats
    """
    # Filter rows where both latencies are below threshold
    filtered_df = stats_df[
        (stats_df['volga_server_p95'] <= latency_threshold) & 
        (stats_df['volga_db_p95'] <= latency_threshold)
    ]
    
    if filtered_df.empty:
        return None
    
    # Find the row with maximum RPS
    max_rps_row = filtered_df.loc[filtered_df['volga_qps'].idxmax()]
    
    # Extract relevant stats
    result = {
        'max_rps': max_rps_row['volga_qps'],
        'end_to_end_p95': max_rps_row['volga_server_p95'],
        'end_to_end_avg': max_rps_row['volga_server_avg'],
        'db_p95': max_rps_row['volga_db_p95'],
        'db_avg': max_rps_row['volga_db_avg'],
        'locust_rps': max_rps_row.get('locust_total_rps', None),
        'locust_p95': max_rps_row.get('locust_cur_p95', None),
        'locust_p50': max_rps_row.get('locust_cur_p50', None),
        'cpu_usage': max_rps_row.get('container_avg', None),
        'worker_cpu': max_rps_row.get('locust_avg_worker_cpu', None),
        'timestamp': max_rps_row['timestamp']
    }
    
    return result

def analyze_benchmarks(
    data_dir: str, 
    start_index: int, 
    end_index: int, 
    except_indexes: List[int],
    latency_threshold: float
) -> List[Dict[str, Any]]:
    """
    Analyze multiple benchmark files and find the max RPS below the latency threshold.
    
    Args:
        data_dir: Directory containing benchmark files
        start_index: Start index (0 = most recent)
        end_index: End index (inclusive)
        except_indexes: List of indexes to exclude
        latency_threshold: Latency threshold in ms
        
    Returns:
        List of results, one per file
    """
    # Find all benchmark files
    files = glob.glob(os.path.join(data_dir, 'run-*.json'))
    if not files:
        raise FileNotFoundError(f"No benchmark files found in {data_dir}")
    
    # Sort by modification time (newest first)
    files.sort(key=os.path.getmtime, reverse=True)
    
    # Validate indexes
    if start_index < 0 or end_index >= len(files) or start_index > end_index:
        raise ValueError(f"Invalid index range: {start_index}-{end_index}, available files: {len(files)}")
    
    # Filter files by index range and exclude specified indexes
    selected_files = []
    for i in range(start_index, end_index + 1):
        if i not in except_indexes and i < len(files):
            selected_files.append((i, files[i]))
    
    if not selected_files:
        raise ValueError("No files selected after applying filters")
    
    # Analyze each file
    results = []
    for idx, file_path in selected_files:
        try:
            print(f"Processing file {idx}: {os.path.basename(file_path)}")
            metadata, stats_df = load_benchmark_file(file_path)
            
            # Find max RPS below threshold
            max_rps_data = find_max_rps_below_threshold(stats_df, latency_threshold)
            
            if max_rps_data:
                # Add metadata
                result = {
                    'file_index': idx,
                    'run_id': metadata.get('run_id', ''),
                    'memory_backend': metadata.get('memory_backend', ''),
                    'num_keys': metadata.get('num_keys', 0),
                    'num_workers': metadata.get('num_workers', 1),  # Default to 1 if not specified
                    **max_rps_data
                }
                results.append(result)
            else:
                print(f"  No data points below latency threshold {latency_threshold}ms")
        except Exception as e:
            print(f"Error processing {file_path}: {e}")
    
    return results

def visualize_results(results: List[Dict[str, Any]], output_file: Optional[str] = None, latency_threshold: float = 100):
    """
    Visualize the results from multiple benchmark files.
    
    Args:
        results: List of results from analyze_benchmarks
        output_file: Optional file path to save the visualization
        latency_threshold: Latency threshold used for analysis
    """
    if not results:
        print("No results to visualize")
        return
    
    # Convert to DataFrame for easier plotting
    df = pd.DataFrame(results)
    
    # Group by memory backend
    backends = df['memory_backend'].unique()
    
    # Create figure with subplots - just 2 plots now with smaller overall size
    fig, axes = plt.subplots(1, 2, figsize=(12, 5))  # Reduced from (15, 6)
    
    # Add a title to the figure
    fig.suptitle('Volga On-Demand Scalability', fontsize=14, y=0.98)
    plt.subplots_adjust(top=0.92, wspace=0.25, left=0.08, right=0.92)
    
    # Plot 1: Max RPS vs Num Workers
    ax = axes[0]
    for backend in backends:
        backend_df = df[df['memory_backend'] == backend]
        # Sort by num_workers to ensure lines don't intersect
        backend_df = backend_df.sort_values('num_workers')
        ax.plot(backend_df['num_workers'], backend_df['max_rps'], 'o-', label=f"{backend}")
    
    ax.set_title(f'Max RPS (latency_threshold_ms={latency_threshold})')
    ax.set_xlabel('Number of Workers')
    ax.set_ylabel('Max RPS')
    ax.grid(True, alpha=0.3)
    
    # Add legend for the first plot to identify backends
    ax.legend(loc='best', fontsize=9)  # Reduced font size
    
    # Make the plot area smaller to create more whitespace
    box = ax.get_position()
    ax.set_position([box.x0, box.y0, box.width * 0.95, box.height * 0.9])
    
    # Plot 2: End-to-End Latency vs Num Workers
    ax = axes[1]
    for backend in backends:
        backend_df = df[df['memory_backend'] == backend]
        # Sort by num_workers to ensure lines don't intersect
        backend_df = backend_df.sort_values('num_workers')
        
        # End-to-end latency
        ax.plot(backend_df['num_workers'], backend_df['end_to_end_p95'], 'o-', label="E2E P95")
        ax.plot(backend_df['num_workers'], backend_df['end_to_end_avg'], 'o--', label="E2E Avg")
        
        # DB latency
        ax.plot(backend_df['num_workers'], backend_df['db_p95'], 's-', label="DB P95")
        ax.plot(backend_df['num_workers'], backend_df['db_avg'], 's--', label="DB Avg")
    
    ax.set_title('Latency at Max RPS')
    ax.set_xlabel('Number of Workers')
    ax.set_ylabel('Latency (ms)')
    ax.grid(True, alpha=0.3)
    ax.legend(loc='best', fontsize=9)  # Reduced font size
    
    # Make the plot area smaller to create more whitespace
    box = ax.get_position()
    ax.set_position([box.x0, box.y0, box.width * 0.95, box.height * 0.9])
    
    # Print summary table - sort by memory_backend and num_workers
    print("\nSummary of results:")
    sorted_df = df.sort_values(['memory_backend', 'num_workers'])
    print(sorted_df[['file_index', 'memory_backend', 'num_workers', 'num_keys', 'max_rps', 
                     'end_to_end_p95', 'end_to_end_avg', 'db_p95', 'db_avg']].to_string(index=False))
    
    # Save to file if requested
    if output_file:
        plt.savefig(output_file, dpi=300, bbox_inches='tight')
        print(f"Visualization saved to {output_file}")
    
    plt.show()
    
    return fig

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Analyze multiple benchmark files')
    parser.add_argument('--data-dir', default='volga_on_demand_perf_benchmarks', 
                        help='Directory containing benchmark files')
    parser.add_argument('--start-index', type=int, default=0, 
                        help='Start index (0 = most recent)')
    parser.add_argument('--end-index', type=int, default=10, 
                        help='End index (inclusive)')
    parser.add_argument('--except', dest='except_indexes', default='', 
                        help='Comma-separated list of indexes to exclude')
    parser.add_argument('--latency-threshold', type=float, default=100, 
                        help='Latency threshold in ms')
    parser.add_argument('--output', help='Output file path (PNG format)')
    
    args = parser.parse_args()
    
    # Parse except indexes
    except_indexes = []
    if args.except_indexes:
        except_indexes = [int(idx) for idx in args.except_indexes.split(',')]
    
    # Analyze benchmarks
    results = analyze_benchmarks(
        args.data_dir,
        args.start_index,
        args.end_index,
        except_indexes,
        args.latency_threshold
    )
    
    # Visualize results
    visualize_results(results, args.output, args.latency_threshold) 