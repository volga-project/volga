import json
import os
import glob
import pandas as pd
import matplotlib.pyplot as plt
import matplotlib.dates as mdates
from datetime import datetime
import numpy as np

def visualize_benchmark_data(run_id=None, data_dir='volga_on_demand_perf_benchmarks', output_file=None, 
                            height_per_plot=3, width=10, display=True):
    """
    Visualize benchmark data from the specified run or the latest run if not specified.
    
    Args:
        run_id: Optional specific run ID to visualize
        data_dir: Directory containing benchmark data files
        output_file: Optional file path to save the visualization (PNG format)
        height_per_plot: Height in inches for each subplot
        width: Width in inches for the figure
        display: Whether to display the plot (set to False when running in headless mode)
    
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
    
    # Extract data from the format in the sample file
    # Each item in the list has a 'stats' dict and a 'timestamp'
    timestamps = []
    locust_data = []
    container_insights_data = []
    volga_on_demand_data = []
    
    for item in data:
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
    
    # Create the visualization - 2x2 grid layout
    fig, axes = plt.subplots(2, 2, figsize=(width*2, height_per_plot*2), sharex=True, 
                            gridspec_kw={'hspace': 0.3, 'wspace': 0.3})
    
    # Rearranged layout:
    # [0,0]: Volga On-Demand Performance
    # [1,0]: Locust Performance Metrics
    # [0,1]: Volga On-Demand CPU Usage (renamed from Container CPU)
    # [1,1]: Locust Worker CPU Usage
    
    # 1. Volga on-demand: combined QPS and latency [0,0]
    if not volga_on_demand_df.empty:
        ax = axes[0, 0]
        ax.set_title('Volga On-Demand Performance')
        
        # Primary y-axis for QPS
        if 'qps' in volga_on_demand_df.columns:
            ax.plot(volga_on_demand_df['elapsed_time'], volga_on_demand_df['qps'], 'c-', label='QPS')
            
            if 'qps_stdev' in volga_on_demand_df.columns:
                ax.fill_between(
                    volga_on_demand_df['elapsed_time'],
                    volga_on_demand_df['qps'] - volga_on_demand_df['qps_stdev'],
                    volga_on_demand_df['qps'] + volga_on_demand_df['qps_stdev'],
                    alpha=0.2, color='c'
                )
        
        ax.set_ylabel('Queries per Second')
        ax.grid(True, alpha=0.3)
        
        # Secondary y-axis for latency
        ax2 = ax.twinx()
        
        if 'server_p99' in volga_on_demand_df.columns:
            ax2.plot(volga_on_demand_df['elapsed_time'], volga_on_demand_df['server_p99'], 'm-', label='P99 Latency')
        
        if 'server_avg' in volga_on_demand_df.columns:
            ax2.plot(volga_on_demand_df['elapsed_time'], volga_on_demand_df['server_avg'], 'y-', label='Avg Latency')
        
        ax2.set_ylabel('Latency (ms)')
        
        # Combine legends
        lines1, labels1 = ax.get_legend_handles_labels()
        lines2, labels2 = ax2.get_legend_handles_labels()
        ax.legend(lines1 + lines2, labels1 + labels2, loc='upper left')
    
    # 2. Locust stats: total_rps, cur_p95 and total_fail_per_sec [1,0]
    if not locust_df.empty:
        ax = axes[1, 0]
        ax.set_title('Locust Performance Metrics')
        
        # Primary y-axis for RPS
        if 'total_rps' in locust_df.columns:
            ax.plot(locust_df['elapsed_time'], locust_df['total_rps'], 'b-', label='Total RPS')
        
        if 'total_fail_per_sec' in locust_df.columns:
            ax.plot(locust_df['elapsed_time'], locust_df['total_fail_per_sec'], 'r-', label='Failures/sec')
        
        ax.set_ylabel('Requests per Second')
        ax.grid(True, alpha=0.3)
        
        # Secondary y-axis for latency
        if 'cur_p95' in locust_df.columns:
            ax2 = ax.twinx()
            ax2.plot(locust_df['elapsed_time'], locust_df['cur_p95'], 'g-', label='P95 Latency (ms)')
            ax2.set_ylabel('Latency (ms)')
            
            # Combine legends
            lines1, labels1 = ax.get_legend_handles_labels()
            lines2, labels2 = ax2.get_legend_handles_labels()
            ax.legend(lines1 + lines2, labels1 + labels2, loc='upper left')
        else:
            ax.legend(loc='upper left')
    
    # 3. Container insights: avg and stdev CPU utilization [0,1] - renamed
    if not container_insights_df.empty:
        ax = axes[0, 1]
        ax.set_title('Volga On-Demand CPU Usage')  # Renamed from Container CPU Utilization
        
        if 'avg' in container_insights_df.columns:
            ax.plot(container_insights_df['elapsed_time'], container_insights_df['avg'], 'b-', label='Avg CPU')
        
        if 'stdev' in container_insights_df.columns:
            ax.fill_between(
                container_insights_df['elapsed_time'],
                container_insights_df['avg'] - container_insights_df['stdev'],
                container_insights_df['avg'] + container_insights_df['stdev'],
                alpha=0.2, color='b'
            )
        
        ax.set_ylabel('CPU Utilization (%)')
        ax.grid(True, alpha=0.3)
        ax.legend(loc='upper left')
    
    # 4. Locust worker stats: avg_worker_cpu, stdev_worker_cpu [1,1]
    if not locust_df.empty:
        ax = axes[1, 1]
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
    plt.tight_layout()
    
    # Extract run information from filename if possible
    try:
        parts = run_id.split('-')
        if len(parts) >= 3:
            timestamp, max_rps, memory_backend = parts
            metadata_text = f"Memory Backend: {memory_backend} | Max RPS: {max_rps} | Timestamp: {timestamp}"
            fig.text(0.5, 0.01, metadata_text, ha='center', fontsize=10)
    except:
        pass
    
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
    
    args = parser.parse_args()
    
    visualize_benchmark_data(
        run_id=args.run_id,
        data_dir=args.data_dir,
        output_file=args.output,
        height_per_plot=args.height,
        width=args.width,
        display=not args.no_display
    ) 