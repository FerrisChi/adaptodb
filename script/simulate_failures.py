# failure_test.py
import docker
import subprocess
import time
import random
import logging
from dataclasses import dataclass
from typing import List, Dict

@dataclass
class NodeInfo:
    container: docker.models.containers.Container
    network_interface: str
    pid: int

class NetworkFailureSimulator:
    def __init__(self):
        self.tc_commands = {
            'latency': 'tc qdisc add dev {interface} root netem delay {delay}ms {jitter}ms distribution normal',
            'packet_loss': 'tc qdisc add dev {interface} root netem loss {loss}%',
            'corruption': 'tc qdisc add dev {interface} root netem corrupt {corrupt}%',
            'reset': 'tc qdisc del dev {interface} root'
        }

    def add_latency(self, interface: str, delay: int, jitter: int):
        cmd = self.tc_commands['latency'].format(interface=interface, delay=delay, jitter=jitter)
        subprocess.run(['sudo', *cmd.split()], check=True)

    def add_packet_loss(self, interface: str, loss: float):
        cmd = self.tc_commands['packet_loss'].format(interface=interface, loss=loss)
        subprocess.run(['sudo', *cmd.split()], check=True)

    def reset(self, interface: str):
        cmd = self.tc_commands['reset'].format(interface=interface)
        subprocess.run(['sudo', *cmd.split()], check=True)

class AdaptoDBFailureTester:
    def __init__(self):
        self.client = docker.from_env()
        self.nodes: Dict[str, NodeInfo] = {}
        self.network = NetworkFailureSimulator()
        
    def setup(self):
        """Initialize node information"""
        for container in self.client.containers.list(filters={'name': 'node-'}):
            # Get container network interface and PID
            inspect = self.client.api.inspect_container(container.id)
            pid = inspect['State']['Pid']
            net_settings = inspect['NetworkSettings']
            interface = f"veth{container.short_id[:5]}"
            
            self.nodes[container.name] = NodeInfo(
                container=container,
                network_interface=interface,
                pid=pid
            )

    def graceful_shutdown(self, node_name: str):
        """Gracefully stop a node"""
        if node_name in self.nodes:
            self.nodes[node_name].container.stop(timeout=30)

    def simulate_network_issues(self, node_name: str):
        """Simulate various network problems"""
        if node_name in self.nodes:
            node = self.nodes[node_name]
            issue = random.choice(['latency', 'loss', 'partition'])
            
            if issue == 'latency':
                self.network.add_latency(node.network_interface, 
                                       delay=random.randint(100, 1000),
                                       jitter=random.randint(10, 100))
            elif issue == 'loss':
                self.network.add_packet_loss(node.network_interface, 
                                           loss=random.uniform(1, 20))
            else:
                self.nodes[node_name].container.disconnect("adaptodb-net")

    def simulate_resource_exhaustion(self, node_name: str):
        """Simulate CPU/memory pressure"""
        if node_name in self.nodes:
            node = self.nodes[node_name]
            pressure_type = random.choice(['cpu', 'memory', 'io'])
            
            if pressure_type == 'cpu':
                # Use stress-ng for CPU pressure
                subprocess.Popen(['nsenter', '-t', str(node.pid), '-m', '-n', 
                               'stress-ng', '--cpu', '1', '--cpu-load', '90'])
            elif pressure_type == 'memory':
                # Limit container memory
                node.container.update(mem_limit='100m')
            else:
                # IO pressure using stress-ng
                subprocess.Popen(['nsenter', '-t', str(node.pid), '-m', '-n',
                               'stress-ng', '--io', '4'])

    def mixed_failure_scenario(self, duration_seconds: int):
        """Run mixed failure scenarios"""
        start_time = time.time()
        while time.time() - start_time < duration_seconds:
            # Select random nodes for correlated failures
            affected_nodes = random.sample(list(self.nodes.keys()), 
                                         k=random.randint(1, len(self.nodes)))
            
            # Select failure scenario
            scenario = random.choice([
                'network_partition',
                'cascading_shutdown',
                'resource_storm'
            ])
            
            if scenario == 'network_partition':
                for node in affected_nodes:
                    self.simulate_network_issues(node)
                    
            elif scenario == 'cascading_shutdown':
                for node in affected_nodes:
                    self.graceful_shutdown(node)
                    time.sleep(random.uniform(1, 5))
                    
            elif scenario == 'resource_storm':
                for node in affected_nodes:
                    self.simulate_resource_exhaustion(node)
                    
            # Wait between scenarios
            time.sleep(random.uniform(10, 30))

    def cleanup(self):
        """Reset all failure conditions"""
        for node_name, node in self.nodes.items():
            try:
                self.network.reset(node.network_interface)
                node.container.update(mem_limit=0)
                # Kill stress-ng processes
                subprocess.run(['nsenter', '-t', str(node.pid), '-m', '-n',
                             'pkill', 'stress-ng'])
            except Exception as e:
                logging.error(f"Cleanup failed for {node_name}: {e}")

def main():
    logging.basicConfig(level=logging.INFO)
    tester = AdaptoDBFailureTester()
    
    try:
        tester.setup()
        tester.mixed_failure_scenario(duration_seconds=600)
    finally:
        tester.cleanup()

if __name__ == "__main__":
    main()
    