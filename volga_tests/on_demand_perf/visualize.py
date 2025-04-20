import json
import os
import glob
import pandas as pd
import matplotlib.pyplot as plt
import matplotlib.dates as mdates
from datetime import datetime
import numpy as np

def visualize_benchmark_data(run_id=None, data_dir='volga_on_demand_perf_benchmarks', output_file=None, 
                            height_per_plot=3, width=10, display=True, start_time=None, end_time=None,
                            hide_cpu=False):
    """
    Visualize benchmark data from the specified run or the latest run if not specified.
    
    Args:
        run_id: Optional specific run ID to visualize
        data_dir: Directory containing benchmark data files
        output_file: Optional file path to save the visualization (PNG format)
        height_per_plot: Height in inches for each subplot
        width: Width in inches for the figure
        display: Whether to display the plot (set to False when running in headless mode)
        start_time: Optional start time in seconds (0-based) to filter data
        end_time: Optional end time in seconds (0-based) to filter data
        hide_cpu: Whether to hide CPU-related graphs
    
    Returns:
        The figure object for further customization if needed
    """
    # Find the data file to visualize
    if run_id:
        data_file = os.path.join(data_dir, f'run-{run_id}.json')
        if not os.path.exists(data_file):
            raise FileNotFoundError(f"No data file found for run ID: {run_id}")
    else:
        # Find the latest run file
        files = glob.glob(os.path.join(data_dir, 'run-*.json'))
        if not files:
            raise FileNotFoundError(f"No benchmark data files found in {data_dir}")
        
        # Sort by modification time (newest first)
        files.sort(key=os.path.getmtime, reverse=True)
        data_file = files[0]
        
        # Extract run_id from filename
        run_id = os.path.basename(data_file).replace('run-', '').replace('.json', '')
        print(f"Automatically selected latest run: {run_id}")
    
    # Load the data
    with open(data_file, 'r') as f:
        data = json.load(f)
    
    # Extract data based on the new structure with run_metadata and historical_stats
    if not isinstance(data, dict) or 'run_metadata' not in data or 'historical_stats' not in data:
        raise ValueError("Invalid data format: Expected a dictionary with 'run_metadata' and 'historical_stats' keys")
    
    metadata = data['run_metadata']
    historical_stats = data['historical_stats']
    
    # Extract data from historical_stats
    timestamps = []
    locust_data = []
    container_insights_data = []
    volga_on_demand_data = []
    
    for item in historical_stats:
        if 'stats' in item and 'timestamp' in item:
            timestamp = item['timestamp']
            timestamps.append(timestamp)
            
            stats = item['stats']
            
            # Extract locust data
            if 'locust' in stats:
                locust_stats = stats['locust']
                locust_stats['timestamp'] = timestamp
                locust_data.append(locust_stats)
            
            # Extract container insights data
            if 'container_insights' in stats:
                container_stats = stats['container_insights']
                container_stats['timestamp'] = timestamp
                container_insights_data.append(container_stats)
            
            # Extract volga on-demand data
            if 'volga_on_demand' in stats:
                volga_stats = stats['volga_on_demand']
                volga_stats['timestamp'] = timestamp
                volga_on_demand_data.append(volga_stats)
    
    # Convert to DataFrames
    locust_df = pd.DataFrame(locust_data)
    container_insights_df = pd.DataFrame(container_insights_data)
    volga_on_demand_df = pd.DataFrame(volga_on_demand_data)
    
    # Print DataFrame info for debugging
    print(f"Locust stats: {len(locust_df)} rows")
    print(f"Container insights stats: {len(container_insights_df)} rows")
    print(f"Volga on-demand stats: {len(volga_on_demand_df)} rows")
    
    # Convert timestamps to datetime
    for df in [locust_df, container_insights_df, volga_on_demand_df]:
        if not df.empty and 'timestamp' in df.columns:
            df['timestamp'] = pd.to_datetime(df['timestamp'], unit='s')
    
    # Find the earliest timestamp to make everything 0-based
    min_timestamps = []
    for df in [locust_df, container_insights_df, volga_on_demand_df]:
        if not df.empty and 'timestamp' in df.columns:
            min_timestamps.append(df['timestamp'].min())
    
    if min_timestamps:
        t0 = min(min_timestamps)
        # Convert timestamps to seconds from start
        for df in [locust_df, container_insights_df, volga_on_demand_df]:
            if not df.empty and 'timestamp' in df.columns:
                df['elapsed_time'] = (df['timestamp'] - t0).dt.total_seconds()
    
    # Filter data by start and end time if specified
    if start_time is not None or end_time is not None:
        for df in [locust_df, container_insights_df, volga_on_demand_df]:
            if not df.empty and 'elapsed_time' in df.columns:
                if start_time is not None:
                    df.query('elapsed_time >= @start_time', inplace=True)
                if end_time is not None:
                    df.query('elapsed_time <= @end_time', inplace=True)
        
        print(f"Filtered data: start={start_time}s, end={end_time}s")
        print(f"Remaining data points: Locust={len(locust_df)}, Container={len(container_insights_df)}, Volga={len(volga_on_demand_df)}")
    
    # Determine number of rows based on hide_cpu flag
    num_rows = 2 if hide_cpu else 3
    
    # Create the visualization - dynamic grid layout
    fig, axes = plt.subplots(num_rows, 2, figsize=(width*2, height_per_plot*num_rows), sharex=True, 
                            gridspec_kw={'hspace': 0.3, 'wspace': 0.3})
    
    # Add a title with num_workers and memory_backend if available
    if metadata:
        num_workers = metadata.get('num_workers', 'unknown')
        memory_backend = metadata.get('memory_backend', 'unknown')
        fig.suptitle(f'Volga On-Demand Load Test (workers={num_workers}, db={memory_backend})', fontsize=14, y=0.98)
    else:
        fig.suptitle('Volga On-Demand Load Test', fontsize=14, y=0.98)
    
    # Adjust subplot positions to make room for the title
    plt.subplots_adjust(top=0.85)  # Reduced from 0.92 to 0.85 to add more space
    
    # 1. Volga on-demand RPS [0,0]
    if not volga_on_demand_df.empty:
        ax = axes[0, 0]
        ax.set_title('Volga On-Demand RPS')
        
        if 'qps' in volga_on_demand_df.columns:
            ax.plot(volga_on_demand_df['elapsed_time'], volga_on_demand_df['qps'], 'c-', label='RPS')
            
            if 'qps_stdev' in volga_on_demand_df.columns:
                ax.fill_between(
                    volga_on_demand_df['elapsed_time'],
                    volga_on_demand_df['qps'] - volga_on_demand_df['qps_stdev'],
                    volga_on_demand_df['qps'] + volga_on_demand_df['qps_stdev'],
                    alpha=0.2, color='c'
                )
        
        ax.set_ylabel('Requests per Second')
        ax.grid(True, alpha=0.3)
        ax.legend(loc='upper left')
    
    # 2. Volga on-demand Latency [1,0]
    if not volga_on_demand_df.empty:
        ax = axes[1, 0]
        ax.set_title('Volga On-Demand Latency')
        
        # Server latency metrics renamed to End-to-End
        if 'server_p95' in volga_on_demand_df.columns:
            ax.plot(volga_on_demand_df['elapsed_time'], volga_on_demand_df['server_p95'], 'm-', label='End-to-End P95')
        
        if 'server_avg' in volga_on_demand_df.columns:
            ax.plot(volga_on_demand_df['elapsed_time'], volga_on_demand_df['server_avg'], 'm--', label='End-to-End Avg')
        
        # Database latency metrics
        if 'db_p95' in volga_on_demand_df.columns:
            ax.plot(volga_on_demand_df['elapsed_time'], volga_on_demand_df['db_p95'], 'g-', label='DB P95')
        
        if 'db_avg' in volga_on_demand_df.columns:
            ax.plot(volga_on_demand_df['elapsed_time'], volga_on_demand_df['db_avg'], 'g--', label='DB Avg')
        
        ax.set_ylabel('Latency (ms)')
        ax.grid(True, alpha=0.3)
        ax.legend(loc='upper left')
    
    # 3. Volga on-demand CPU usage [2,0] - only if not hidden
    if not hide_cpu and not container_insights_df.empty:
        ax = axes[2, 0]
        ax.set_title('Volga On-Demand CPU Usage')
        
        if 'avg' in container_insights_df.columns:
            ax.plot(container_insights_df['elapsed_time'], container_insights_df['avg'], 'b-', label='Avg Num CPUs')
        
        if 'stdev' in container_insights_df.columns:
            ax.fill_between(
                container_insights_df['elapsed_time'],
                container_insights_df['avg'] - container_insights_df['stdev'],
                container_insights_df['avg'] + container_insights_df['stdev'],
                alpha=0.2, color='b'
            )
        
        ax.set_ylabel('Number of CPUs')
        ax.grid(True, alpha=0.3)
        ax.legend(loc='upper left')
    
    # 4. Locust RPS [0,1]
    if not locust_df.empty:
        ax = axes[0, 1]
        ax.set_title('Locust RPS')
        
        if 'total_rps' in locust_df.columns:
            ax.plot(locust_df['elapsed_time'], locust_df['total_rps'], 'b-', label='Total RPS')
        
        if 'total_fail_per_sec' in locust_df.columns:
            ax.plot(locust_df['elapsed_time'], locust_df['total_fail_per_sec'], 'r-', label='Failures/sec')
        
        ax.set_ylabel('Requests per Second')
        ax.grid(True, alpha=0.3)
        ax.legend(loc='upper left')
    
    # 5. Locust Latency [1,1]
    if not locust_df.empty:
        ax = axes[1, 1]
        ax.set_title('Locust Latency')
        
        if 'cur_p95' in locust_df.columns:
            ax.plot(locust_df['elapsed_time'], locust_df['cur_p95'], 'g-', label='P95 Latency')
        
        if 'cur_p50' in locust_df.columns:
            ax.plot(locust_df['elapsed_time'], locust_df['cur_p50'], 'g--', label='P50 Latency')
        
        ax.set_ylabel('Latency (ms)')
        ax.grid(True, alpha=0.3)
        ax.legend(loc='upper left')
    
    # 6. Locust worker CPU usage [2,1] - only if not hidden
    if not hide_cpu and not locust_df.empty:
        ax = axes[2, 1]
        ax.set_title('Locust Worker CPU Usage')
        
        if 'avg_worker_cpu' in locust_df.columns:
            ax.plot(locust_df['elapsed_time'], locust_df['avg_worker_cpu'], 'g-', label='Avg Worker CPU')
        
        if 'stdev_worker_cpu' in locust_df.columns:
            ax.fill_between(
                locust_df['elapsed_time'],
                locust_df['avg_worker_cpu'] - locust_df['stdev_worker_cpu'],
                locust_df['avg_worker_cpu'] + locust_df['stdev_worker_cpu'],
                alpha=0.2, color='g'
            )
        
        ax.set_ylabel('CPU Usage (%)')
        ax.grid(True, alpha=0.3)
        ax.legend(loc='upper left')
    
    # Format the x-axis to show time in seconds
    for row in axes:
        for ax in row:
            ax.xaxis.set_major_formatter(plt.FuncFormatter(lambda x, pos: f'{int(x)}s'))
    
    plt.xlabel('Time (seconds from start)')
    
    # Save to file if requested
    if output_file:
        plt.savefig(output_file, dpi=300, bbox_inches='tight')
        print(f"Visualization saved to {output_file}")
    
    # Display the plot if requested
    if display:
        plt.show()
    
    return fig

if __name__ == "__main__":
    import argparse
    
    parser = argparse.ArgumentParser(description='Visualize on-demand benchmark data')
    parser.add_argument('--run-id', help='Specific run ID to visualize')
    parser.add_argument('--data-dir', default='volga_on_demand_perf_benchmarks', 
                        help='Directory containing benchmark data files')
    parser.add_argument('--output', help='Output file path (PNG format)')
    parser.add_argument('--height', type=float, default=3, help='Height per subplot in inches')
    parser.add_argument('--width', type=float, default=10, help='Figure width in inches')
    parser.add_argument('--no-display', action='store_true', help='Do not display the plot (useful for batch processing)')
    parser.add_argument('-i', '--index', type=int, default=0, 
                        help='Index of file to open (0=most recent, 1=second most recent, etc.)')
    parser.add_argument('--start', type=float, help='Start time in seconds (0-based) to filter data')
    parser.add_argument('--end', type=float, help='End time in seconds (0-based) to filter data')
    parser.add_argument('--hide-cpu', action='store_true', help='Hide CPU-related graphs')
    
    args = parser.parse_args()
    
    # If both run-id and index are provided, run-id takes precedence
    if args.run_id:
        run_id = args.run_id
    else:
        # Find all run files
        files = glob.glob(os.path.join(args.data_dir, 'run-*.json'))
        if not files:
            raise FileNotFoundError(f"No benchmark data files found in {args.data_dir}")
        
        # Sort by modification time (newest first)
        files.sort(key=os.path.getmtime, reverse=True)
        
        # Check if index is valid
        if args.index < 0 or args.index >= len(files):
            raise ValueError(f"Index {args.index} is out of range. There are {len(files)} files.")
        
        # Get the file at the specified index
        selected_file = files[args.index]
        run_id = os.path.basename(selected_file).replace('run-', '').replace('.json', '')
        print(f"Selected file at index {args.index}: {run_id}")
    
    visualize_benchmark_data(
        run_id=run_id,
        data_dir=args.data_dir,
        output_file=args.output,
        height_per_plot=args.height,
        width=args.width,
        display=not args.no_display,
        start_time=args.start,
        end_time=args.end,
        hide_cpu=args.hide_cpu
    ) 