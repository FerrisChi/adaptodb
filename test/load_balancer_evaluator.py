import json
import time
import subprocess
import matplotlib.pyplot as plt
import signal
import argparse

# Flag to control the data collection loop
stop_signal = False

def fetch_entries(cluster_port):
    try:
        command = ["grpcurl", "-plaintext", "-d", "{}", f"localhost:{cluster_port}", "proto.NodeStats/GetStats"]
        result = subprocess.run(command, capture_output=True, text=True)
        if result.returncode != 0:
            print(f"Error fetching data from cluster on port {cluster_port}: {result.stderr}")
            return None
        response = json.loads(result.stdout)
        return int(response.get("entries", 0)) if response.get("entries") is not None else 0
    except Exception as e:
        print(f"Exception while fetching data: {e}")
        return None

def calculate_ports(num_clusters, nodes_per_cluster, base_port):
    ports = []
    for cluster_index in range(num_clusters):
        cluster_port = base_port + (cluster_index * nodes_per_cluster)
        ports.append(cluster_port)
    return ports

def visualize_load(clusters, interval, duration):
    global stop_signal
    end_time = time.time() + duration
    cluster_data = {cluster: [] for cluster in clusters}
    timestamps = []

    print("Starting data collection (press 'Ctrl+C' to stop)...")

    plt.ion()  # Enable interactive mode
    fig, ax = plt.subplots()

    try:
        while time.time() < end_time:
            if stop_signal:
                print("\nStopping data collection...")
                break

            current_time = time.strftime("%H:%M:%S", time.localtime())
            timestamps.append(current_time)

            for cluster in clusters:
                entries = fetch_entries(cluster)
                print(f"\nTime: {current_time}")
                print(f"Cluster {cluster}: {entries} entries fetched.")

                if entries is not None:
                    cluster_data[cluster].append(entries)
                else:
                    cluster_data[cluster].append(0)

            # Update plot
            ax.clear()
            for cluster, data in cluster_data.items():
                ax.plot(timestamps, data, label=f"Cluster {cluster}")

            ax.set_xlabel("Time")
            ax.set_ylabel("Number of Entries")
            ax.set_title("Load Across Clusters Over Time")
            ax.legend()
            plt.xticks(rotation=45)
            plt.tight_layout()
            plt.pause(0.1)

            time.sleep(interval)

    except KeyboardInterrupt:
        print("\nCtrl+C pressed. Stopping data collection...")

    print("Visualizing collected data...")
    plt.ioff()  # Disable interactive mode
    plt.show()

def signal_handler(sig, frame):
    global stop_signal
    stop_signal = True

if __name__ == "__main__":
    signal.signal(signal.SIGINT, signal_handler)

    parser = argparse.ArgumentParser(description="Cluster Data Collection and Visualization")
    parser.add_argument("--num_clusters", type=int, required=True, help="Number of clusters")
    parser.add_argument("--nodes_per_cluster", type=int, required=True, help="Number of nodes per cluster")
    parser.add_argument("--interval", type=int, required=True, help="Time interval (in seconds) between data fetches")
    parser.add_argument("--duration", type=int, required=True, help="Total duration (in seconds) for data collection")
    parser.add_argument("--base_port", type=int, default=53001, help="Base port number for clusters (default: 53000)")

    args = parser.parse_args()

    clusters = calculate_ports(args.num_clusters, args.nodes_per_cluster, args.base_port)
    print(f"Calculated cluster ports: {clusters}")

    visualize_load(clusters, args.interval, args.duration)
