import json
import time
import threading
from datetime import datetime, timedelta
from collections import defaultdict, deque
from confluent_kafka import Consumer, Producer
import pandas as pd
import numpy as np

class RealTimeProcessor:
    def __init__(self):
        self.bootstrap_servers = 'localhost:9092'
        
        # Consumers
        self.log_consumer = Consumer({
            'bootstrap.servers': self.bootstrap_servers,
            'group.id': 'log-processor',
            'auto.offset.reset': 'latest'
        })
        self.log_consumer.subscribe(['log-events'])
        
        self.metrics_consumer = Consumer({
            'bootstrap.servers': self.bootstrap_servers,
            'group.id': 'metrics-processor',
            'auto.offset.reset': 'latest'
        })
        self.metrics_consumer.subscribe(['metrics-events'])
        
        # Producer for processed data
        self.producer = Producer({
            'bootstrap.servers': self.bootstrap_servers
        })
        
        # In-memory storage for real-time aggregation
        self.log_aggregates = defaultdict(lambda: defaultdict(int))
        self.metrics_aggregates = defaultdict(lambda: defaultdict(list))
        self.time_window_data = defaultdict(lambda: deque(maxlen=100))  # Last 100 data points
        
        # Lock for thread safety
        self.lock = threading.Lock()
        
        # Running flag
        self.running = False
        
    def start_processing(self):
        """Start the real-time processing threads"""
        self.running = True
        
        # Start consumer threads
        log_thread = threading.Thread(target=self.process_log_events)
        metrics_thread = threading.Thread(target=self.process_metrics_events)
        aggregation_thread = threading.Thread(target=self.periodic_aggregation)
        
        log_thread.daemon = True
        metrics_thread.daemon = True
        aggregation_thread.daemon = True
        
        log_thread.start()
        metrics_thread.start()
        aggregation_thread.start()
        
        print("Real-time processor started...")
        return log_thread, metrics_thread, aggregation_thread
    
    def process_log_events(self):
        """Process incoming log events"""
        print("Starting log event processing...")
        
        while self.running:
            msg = self.log_consumer.poll(1.0)
            
            if msg is None:
                continue
            if msg.error():
                print(f"Consumer error: {msg.error()}")
                continue
                
            try:
                log_data = json.loads(msg.value().decode('utf-8'))
                timestamp = log_data['timestamp']
                service = log_data['service']
                level = log_data['level']
                
                with self.lock:
                    # Aggregate by service and log level
                    current_minute = int(timestamp // 60000) * 60000  # Round to minute
                    key = f"{service}_{level}_{current_minute}"
                    
                    self.log_aggregates[service][level] += 1
                    self.time_window_data[f"logs_{service}_{level}"].append({
                        'timestamp': timestamp,
                        'count': 1
                    })
                    
                    # Detect anomalies (simple threshold-based)
                    if level == 'ERROR' and self.log_aggregates[service]['ERROR'] > 10:
                        self.send_alert(f"High error rate detected in {service}")
                        
            except Exception as e:
                print(f"Error processing log event: {e}")
                
    def process_metrics_events(self):
        """Process incoming metrics events"""
        print("Starting metrics event processing...")
        
        while self.running:
            msg = self.metrics_consumer.poll(1.0)
            
            if msg is None:
                continue
            if msg.error():
                print(f"Consumer error: {msg.error()}")
                continue
                
            try:
                metrics_data = json.loads(msg.value().decode('utf-8'))
                timestamp = metrics_data['timestamp']
                service = metrics_data['service']
                
                with self.lock:
                    # Store metrics for aggregation
                    for metric in ['cpuUsage', 'memoryUsage', 'requestCount', 'responseTime']:
                        if metric in metrics_data:
                            self.metrics_aggregates[service][metric].append({
                                'timestamp': timestamp,
                                'value': metrics_data[metric]
                            })
                            
                            # Keep only last 1000 entries per metric
                            if len(self.metrics_aggregates[service][metric]) > 1000:
                                self.metrics_aggregates[service][metric] = \
                                    self.metrics_aggregates[service][metric][-1000:]
                    
                    # Real-time anomaly detection
                    if metrics_data['cpuUsage'] > 80:
                        self.send_alert(f"High CPU usage in {service}: {metrics_data['cpuUsage']:.2f}%")
                    
                    if metrics_data['responseTime'] > 1000:
                        self.send_alert(f"High response time in {service}: {metrics_data['responseTime']:.2f}ms")
                        
            except Exception as e:
                print(f"Error processing metrics event: {e}")
    
    def periodic_aggregation(self):
        """Perform periodic aggregation and send results"""
        print("Starting periodic aggregation...")
        
        while self.running:
            try:
                time.sleep(10)  # Aggregate every 10 seconds
                
                with self.lock:
                    aggregated_data = self.compute_aggregations()
                    
                    if aggregated_data:
                        # Send aggregated data to output topic
                        self.producer.produce('processed-analytics', 
                                            json.dumps(aggregated_data).encode('utf-8'))
                        self.producer.flush()
                        print(f"Sent aggregated data: {len(aggregated_data)} metrics")
                        
            except Exception as e:
                print(f"Error in periodic aggregation: {e}")
    
    def compute_aggregations(self):
        """Compute various aggregations from the collected data"""
        aggregated = {
            'timestamp': int(time.time() * 1000),
            'log_aggregates': {},
            'metrics_aggregates': {},
            'alerts': []
        }
        
        # Aggregate log data
        for service, levels in self.log_aggregates.items():
            aggregated['log_aggregates'][service] = dict(levels)
        
        # Aggregate metrics data
        for service, metrics in self.metrics_aggregates.items():
            service_aggregates = {}
            
            for metric_name, metric_data in metrics.items():
                if metric_data:
                    values = [point['value'] for point in metric_data[-60:]]  # Last 60 points
                    
                    if values:
                        service_aggregates[metric_name] = {
                            'avg': float(np.mean(values)),
                            'min': float(np.min(values)),
                            'max': float(np.max(values)),
                            'std': float(np.std(values)),
                            'count': int(len(values)),
                            'latest': float(values[-1])
                        }
            
            if service_aggregates:
                aggregated['metrics_aggregates'][service] = service_aggregates
        
        return aggregated
    
    def send_alert(self, message):
        """Send alert for anomalies"""
        alert = {
            'timestamp': int(time.time() * 1000),
            'type': 'ALERT',
            'message': message,
            'severity': 'HIGH'
        }
        
        try:
            self.producer.produce('processed-analytics', 
                                json.dumps(alert).encode('utf-8'))
            self.producer.flush()
            print(f"ALERT: {message}")
        except Exception as e:
            print(f"Error sending alert: {e}")
    
    def get_current_state(self):
        """Get current aggregated state for API"""
        with self.lock:
            return self.compute_aggregations()
    
    def stop_processing(self):
        """Stop the processor"""
        self.running = False
        self.log_consumer.close()
        self.metrics_consumer.close()
        self.producer.close()
        print("Processor stopped")

if __name__ == "__main__":
    processor = RealTimeProcessor()
    
    try:
        threads = processor.start_processing()
        
        # Keep the main thread alive
        while True:
            time.sleep(1)
            
    except KeyboardInterrupt:
        print("\nStopping processor...")
        processor.stop_processing()
        print("Processor stopped successfully")