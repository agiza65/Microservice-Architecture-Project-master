import requests
import json
import time
from kafka import KafkaConsumer
import threading

class SystemTester:
    def __init__(self):
        self.api_base_url = "http://localhost:5000/api"
        self.kafka_bootstrap_servers = ['localhost:9092']
        self.test_results = []
    
    def test_api_endpoints(self):
        """Test all API endpoints"""
        print("Testing API endpoints...")
        
        endpoints = [
            ('/metrics', 'Current metrics'),
            ('/metrics/historical/1', 'Historical metrics'),
            ('/alerts', 'Alerts'),
            ('/services', 'Services list')
        ]
        
        for endpoint, description in endpoints:
            try:
                response = requests.get(f"{self.api_base_url}{endpoint}", timeout=10)
                if response.status_code == 200:
                    data = response.json()
                    print(f"✓ {description}: OK ({len(str(data))} bytes)")
                    self.test_results.append(f"API {endpoint}: PASS")
                else:
                    print(f"✗ {description}: HTTP {response.status_code}")
                    self.test_results.append(f"API {endpoint}: FAIL")
            except Exception as e:
                print(f"✗ {description}: Error - {str(e)}")
                self.test_results.append(f"API {endpoint}: ERROR")
    
    def test_kafka_connectivity(self):
        """Test Kafka connectivity"""
        print("\nTesting Kafka connectivity...")
        
        topics = ['log-events', 'metrics-events', 'processed-analytics']
        
        for topic in topics:
            try:
                consumer = KafkaConsumer(
                    topic,
                    bootstrap_servers=self.kafka_bootstrap_servers,
                    consumer_timeout_ms=5000,
                    auto_offset_reset='latest'
                )
                
                # Try to get metadata
                partitions = consumer.partitions_for_topic(topic)
                if partitions:
                    print(f"✓ Topic '{topic}': Available ({len(partitions)} partitions)")
                    self.test_results.append(f"Kafka {topic}: PASS")
                else:
                    print(f"✗ Topic '{topic}': No partitions found")
                    self.test_results.append(f"Kafka {topic}: FAIL")
                
                consumer.close()
                
            except Exception as e:
                print(f"✗ Topic '{topic}': Error - {str(e)}")
                self.test_results.append(f"Kafka {topic}: ERROR")
    
    def test_data_flow(self):
        """Test end-to-end data flow"""
        print("\nTesting data flow...")
        
        try:
            # Test if we're receiving processed analytics
            consumer = KafkaConsumer(
                'processed-analytics',
                bootstrap_servers=self.kafka_bootstrap_servers,
                value_deserializer=lambda x: json.loads(x.decode('utf-8')),
                consumer_timeout_ms=15000,
                auto_offset_reset='latest'
            )
            
            print("Waiting for processed analytics data...")
            message_count = 0
            
            for message in consumer:
                message_count += 1
                data = message.value
                print(f"  Received message {message_count}: {data.get('timestamp', 'Unknown timestamp')}")
                
                if message_count >= 2:  # Wait for at least 2 messages
                    break
            
            if message_count > 0:
                print(f"✓ Data flow: OK (received {message_count} messages)")
                self.test_results.append("Data flow: PASS")
            else:
                print("✗ Data flow: No messages received")
                self.test_results.append("Data flow: FAIL")
            
            consumer.close()
            
        except Exception as e:
            print(f"✗ Data flow: Error - {str(e)}")
            self.test_results.append("Data flow: ERROR")
    
    def test_dashboard_accessibility(self):
        """Test dashboard accessibility"""
        print("\nTesting dashboard accessibility...")
        
        try:
            response = requests.get("http://localhost:5000", timeout=10)
            if response.status_code == 200 and "Real-Time Analytics Dashboard" in response.text:
                print("✓ Dashboard: Accessible")
                self.test_results.append("Dashboard: PASS")
            else:
                print("✗ Dashboard: Not accessible or content missing")
                self.test_results.append("Dashboard: FAIL")
        except Exception as e:
            print(f"✗ Dashboard: Error - {str(e)}")
            self.test_results.append("Dashboard: ERROR")
    
    def generate_test_report(self):
        """Generate a test report"""
        print("\n" + "="*50)
        print("           TEST REPORT")
        print("="*50)
        
        passed = sum(1 for result in self.test_results if "PASS" in result)
        failed = sum(1 for result in self.test_results if "FAIL" in result)
        errors = sum(1 for result in self.test_results if "ERROR" in result)
        total = len(self.test_results)
        
        print(f"Total Tests: {total}")
        print(f"Passed: {passed}")
        print(f"Failed: {failed}")
        print(f"Errors: {errors}")
        print()
        
        for result in self.test_results:
            status = "✓" if "PASS" in result else "✗"
            print(f"{status} {result}")
        
        print("\n" + "="*50)
        
        if failed + errors == 0:
            print("🎉 ALL TESTS PASSED! System is working correctly.")
        else:
            print("⚠️  Some tests failed. Check the system components.")
        
        return passed, failed, errors
    
    def run_all_tests(self):
        """Run all system tests"""
        print("Starting Real-Time Analytics Engine System Tests")
        print("="*50)
        
        self.test_kafka_connectivity()
        self.test_api_endpoints()
        self.test_dashboard_accessibility()
        self.test_data_flow()
        
        return self.generate_test_report()

def monitor_system_health():
    """Monitor system health continuously"""
    print("\nStarting continuous health monitoring...")
    print("Press Ctrl+C to stop monitoring")
    
    try:
        while True:
            try:
                # Check API health
                response = requests.get("http://localhost:5000/api/metrics", timeout=5)
                if response.status_code == 200:
                    data = response.json()
                    services_count = len(data.get('metrics_aggregates', {}))
                    alerts_count = len(data.get('alerts', []))
                    timestamp = data.get('timestamp', 0)
                    
                    print(f"[{time.strftime('%H:%M:%S')}] Health OK - Services: {services_count}, Alerts: {alerts_count}, Last Update: {timestamp}")
                else:
                    print(f"[{time.strftime('%H:%M:%S')}] Health WARNING - API responded with status {response.status_code}")
                
            except Exception as e:
                print(f"[{time.strftime('%H:%M:%S')}] Health ERROR - {str(e)}")
            
            time.sleep(10)  # Check every 10 seconds
            
    except KeyboardInterrupt:
        print("\nHealth monitoring stopped.")

if __name__ == "__main__":
    import sys
    
    tester = SystemTester()
    
    if len(sys.argv) > 1 and sys.argv[1] == "monitor":
        monitor_system_health()
    else:
        passed, failed, errors = tester.run_all_tests()
        
        if failed + errors == 0:
            print("\nSystem is ready for production!")
        else:
            print("\nPlease fix the failing components before proceeding.")
        
        # Offer to start monitoring
        choice = input("\nWould you like to start health monitoring? (y/n): ")
        if choice.lower() == 'y':
            monitor_system_health()