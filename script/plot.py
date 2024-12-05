import pandas as pd
import matplotlib.pyplot as plt
import glob
import re
from datetime import datetime
import numpy as np

def extract_workers_and_size(filename):
    """Extract the number of workers and data size from the filename (e.g., 'metrics_16_4k.csv')"""
    match = re.search(r'metrics_(\d+)_(\d+k)\.csv', filename)
    if match:
        return int(match.group(1)), match.group(2)
    return None, None

def process_metrics_file(filepath):
    """Process a single metrics file and return processed data"""
    # Extract workers and data size from filename
    workers, data_size = extract_workers_and_size(filepath)
    if not workers or not data_size:
        return None
    
    # Read the CSV file
    df = pd.read_csv(filepath)
    
    # Convert timestamp to datetime
    df['Timestamp'] = pd.to_datetime(df['Timestamp'])
    
    # Calculate relative time in seconds from the first timestamp
    first_timestamp = df['Timestamp'].min()
    df['Relative_Time'] = (df['Timestamp'] - first_timestamp).dt.total_seconds()
    
    # Calculate read and write throughput
    read_phase = df[df['Successful Reads'] != 0].head(60)
    write_phase = df[df['Successful Writes'] != 0].head(60)  # After sleep
    
    # Calculate average throughput for each phase
    avg_write_throughput = write_phase['Successful Writes'].mean() if not write_phase.empty else 0
    avg_read_throughput = read_phase['Successful Reads'].mean() if not read_phase.empty else 0
    
    # Return processed data
    return {
        'workers': workers,
        'data_size': data_size,
        'avg_write_throughput': avg_write_throughput,
        'avg_read_throughput': avg_read_throughput
    }

def plot_throughput_analysis(metrics_dir='.'):
    """Plot throughput analysis from all metrics files in the directory"""
    # Get all metrics files
    files = glob.glob(f'{metrics_dir}/metrics_*.csv')
    
    if not files:
        print("No metrics files found!")
        return
    
    # Process all files
    datasets = []
    for file in sorted(files):
        data = process_metrics_file(file)
        if data:
            datasets.append(data)
    
    if not datasets:
        print("No valid data found in metrics files!")
        return
    
    # Create two subplots
    fig, (ax1, ax2) = plt.subplots(2, 1, figsize=(8, 8))
    
    # Organize data by data size
    data_sizes = sorted(set(d['data_size'] for d in datasets))
    colors = plt.cm.viridis(np.linspace(0, 1, len(data_sizes)))
    
    for data_size, color in zip(data_sizes, colors):
        # Filter data for this size
        size_data = sorted(
            [d for d in datasets if d['data_size'] == data_size],
            key=lambda x: x['workers']
        )
        workers = [d['workers'] for d in size_data]
        write_throughput = [d['avg_write_throughput'] for d in size_data]
        read_throughput = [d['avg_read_throughput'] for d in size_data]
        
        # Plot write throughput
        ax1.plot(workers, write_throughput, 'o-', 
                label=f'Data Size: {data_size}',
                color=color,
                linewidth=2)
        
        # Plot read throughput
        ax2.plot(workers, read_throughput, 'o-', 
                label=f'Data Size: {data_size}',
                color=color,
                linewidth=2)
    
    # Customize write throughput plot
    ax1.set_title('Write Throughput vs Number of Workers')
    ax1.set_xlabel('Number of Workers')
    ax1.set_ylabel('Write Throughput (operations/second)')
    ax1.grid(True, linestyle='--', alpha=0.7)
    ax1.legend()
    
    # Customize read throughput plot
    ax2.set_title('Read Throughput vs Number of Workers')
    ax2.set_xlabel('Number of Workers')
    ax2.set_ylabel('Read Throughput (operations/second)')
    ax2.grid(True, linestyle='--', alpha=0.7)
    ax2.legend()
    
    # Set logarithmic scale for both axes if the data spans multiple orders of magnitude
    # ax1.set_xscale('log', base=2)
    # ax2.set_xscale('log', base=2)
    
    # Adjust layout to prevent label cutoff
    plt.tight_layout()
    
    # Save the plot
    plt.savefig('throughput_analysis.png', dpi=300, bbox_inches='tight')
    print("Plot saved as 'throughput_analysis.png'")
    
    # Show the plot
    plt.show()

if __name__ == "__main__":
    # You can specify the directory containing the metrics files
    # Default is current directory
    plot_throughput_analysis('metrics')