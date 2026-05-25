from flask import Flask, jsonify, render_template_string
from flask_cors import CORS
import json
import time
import threading
from confluent_kafka import Consumer
from collections import defaultdict, deque

app = Flask(__name__)
CORS(app)

class AnalyticsAPI:
    def __init__(self):
        self.current_data = {
            'log_aggregates': {},
            'metrics_aggregates': {},
            'alerts': [],
            'timestamp': int(time.time() * 1000)
        }
        
        # Consumer for processed analytics
        self.consumer = None
        self.running = True
        
        try:
            print("Attempting to connect to Kafka...")
            self.consumer = Consumer({
                'bootstrap.servers': 'localhost:9092',
                'group.id': 'api-consumer',
                'auto.offset.reset': 'latest'
            })
            self.consumer.subscribe(['processed-analytics'])
            print("Successfully connected to Kafka!")
            
            # Historical data storage
            self.historical_data = deque(maxlen=1000)
            self.alerts_history = deque(maxlen=100)
            
            # Start consumer thread
            self.consumer_thread = threading.Thread(target=self.consume_analytics)
            self.consumer_thread.daemon = True
            self.consumer_thread.start()
            
        except Exception as e:
            print(f"Failed to connect to Kafka: {e}")
            print("API server will run without real-time data updates")
            self.consumer = None
            self.historical_data = deque(maxlen=1000)
            self.alerts_history = deque(maxlen=100)
    
    def consume_analytics(self):
        """Consume processed analytics data"""
        if not self.consumer:
            print("No Kafka consumer available")
            return
            
        print("Starting analytics consumer...")
        
        try:
            while self.running:
                msg = self.consumer.poll(1.0)
                
                if msg is None:
                    continue
                if msg.error():
                    print(f"Consumer error: {msg.error()}")
                    continue
                    
                try:
                    data = json.loads(msg.value().decode('utf-8'))
                    
                    if data.get('type') == 'ALERT':
                        self.alerts_history.append(data)
                        self.current_data['alerts'] = list(self.alerts_history)[-10:]  # Last 10 alerts
                    else:
                        self.current_data.update(data)
                        self.historical_data.append(data)
                        
                except Exception as e:
                    print(f"Error consuming analytics: {e}")
        except Exception as e:
            print(f"Error in consumer loop: {e}")
        finally:
            if self.consumer:
                self.consumer.close()
    
    def get_current_metrics(self):
        """Get current metrics"""
        return self.current_data
    
    def get_historical_data(self, hours=1):
        """Get historical data for specified hours"""
        cutoff_time = int(time.time() * 1000) - (hours * 60 * 60 * 1000)
        
        historical = [
            data for data in self.historical_data 
            if data.get('timestamp', 0) > cutoff_time
        ]
        
        return historical
    
    def get_service_metrics(self, service_name):
        """Get metrics for a specific service"""
        current_metrics = self.current_data.get('metrics_aggregates', {})
        return current_metrics.get(service_name, {})
    
    def get_alerts(self):
        """Get recent alerts"""
        return list(self.alerts_history)

# Global analytics instance
analytics = AnalyticsAPI()

# API Routes
@app.route('/api/metrics')
def get_metrics():
    """Get current aggregated metrics"""
    return jsonify(analytics.get_current_metrics())

@app.route('/api/metrics/historical/<int:hours>')
def get_historical_metrics(hours):
    """Get historical metrics for specified hours"""
    data = analytics.get_historical_data(hours)
    return jsonify(data)

@app.route('/api/service/<service_name>')
def get_service_metrics(service_name):
    """Get metrics for a specific service"""
    metrics = analytics.get_service_metrics(service_name)
    return jsonify(metrics)

@app.route('/api/alerts')
def get_alerts():
    """Get recent alerts"""
    alerts = analytics.get_alerts()
    return jsonify(alerts)

@app.route('/api/services')
def get_services():
    """Get list of all services"""
    services = list(analytics.current_data.get('metrics_aggregates', {}).keys())
    return jsonify(services)

@app.route('/api/charts/cpu')
def get_cpu_chart():
    """Get CPU usage chart data in Chart.js format"""
    historical = analytics.get_historical_data(1)  # Last hour
    
    chart_data = defaultdict(list)
    timestamps = []
    
    for data_point in historical:
        timestamp = data_point.get('timestamp', 0)
        # Convert timestamp to readable format
        time_str = time.strftime('%H:%M:%S', time.localtime(timestamp / 1000))
        timestamps.append(time_str)
        
        metrics = data_point.get('metrics_aggregates', {})
        for service, service_metrics in metrics.items():
            cpu_data = service_metrics.get('cpuUsage', {})
            chart_data[service].append(cpu_data.get('avg', 0))
    
    # Create Chart.js compatible data structure
    datasets = []
    colors = ['#FF6384', '#36A2EB', '#FFCE56', '#4BC0C0', '#9966FF', '#FF9F40']
    
    for i, (service, values) in enumerate(chart_data.items()):
        dataset = {
            'label': service,
            'data': values,
            'borderColor': colors[i % len(colors)],
            'backgroundColor': colors[i % len(colors)] + '20',  # 20% opacity
            'fill': False,
            'tension': 0.1
        }
        datasets.append(dataset)
    
    chartjs_data = {
        'labels': timestamps,
        'datasets': datasets
    }
    
    return jsonify(chartjs_data)

@app.route('/api/charts/memory')
def get_memory_chart():
    """Get memory usage chart data in Chart.js format"""
    historical = analytics.get_historical_data(1)  # Last hour
    
    chart_data = defaultdict(list)
    timestamps = []
    
    for data_point in historical:
        timestamp = data_point.get('timestamp', 0)
        time_str = time.strftime('%H:%M:%S', time.localtime(timestamp / 1000))
        timestamps.append(time_str)
        
        metrics = data_point.get('metrics_aggregates', {})
        for service, service_metrics in metrics.items():
            memory_data = service_metrics.get('memoryUsage', {})
            chart_data[service].append(memory_data.get('avg', 0))
    
    # Create Chart.js compatible data structure
    datasets = []
    colors = ['#FF6384', '#36A2EB', '#FFCE56', '#4BC0C0', '#9966FF', '#FF9F40']
    
    for i, (service, values) in enumerate(chart_data.items()):
        dataset = {
            'label': service,
            'data': values,
            'borderColor': colors[i % len(colors)],
            'backgroundColor': colors[i % len(colors)] + '20',
            'fill': False,
            'tension': 0.1
        }
        datasets.append(dataset)
    
    chartjs_data = {
        'labels': timestamps,
        'datasets': datasets
    }
    
    return jsonify(chartjs_data)

@app.route('/api/charts/response-time')
def get_response_time_chart():
    """Get response time chart data in Chart.js format"""
    historical = analytics.get_historical_data(1)  # Last hour
    
    chart_data = defaultdict(list)
    timestamps = []
    
    for data_point in historical:
        timestamp = data_point.get('timestamp', 0)
        time_str = time.strftime('%H:%M:%S', time.localtime(timestamp / 1000))
        timestamps.append(time_str)
        
        metrics = data_point.get('metrics_aggregates', {})
        for service, service_metrics in metrics.items():
            response_time_data = service_metrics.get('responseTime', {})
            chart_data[service].append(response_time_data.get('avg', 0))
    
    # Create Chart.js compatible data structure
    datasets = []
    colors = ['#FF6384', '#36A2EB', '#FFCE56', '#4BC0C0', '#9966FF', '#FF9F40']
    
    for i, (service, values) in enumerate(chart_data.items()):
        dataset = {
            'label': service,
            'data': values,
            'borderColor': colors[i % len(colors)],
            'backgroundColor': colors[i % len(colors)] + '20',
            'fill': False,
            'tension': 0.1
        }
        datasets.append(dataset)
    
    chartjs_data = {
        'labels': timestamps,
        'datasets': datasets
    }
    
    return jsonify(chartjs_data)

# Dashboard HTML Template
DASHBOARD_HTML = """
<!DOCTYPE html>
<html>
<head>
    <title>Real-Time Analytics Dashboard</title>
    <script src="https://cdn.jsdelivr.net/npm/chart.js"></script>
    <style>
        body { font-family: Arial, sans-serif; margin: 20px; background: #f5f5f5; }
        .container { max-width: 1400px; margin: 0 auto; }
        .header { background: #2c3e50; color: white; padding: 20px; border-radius: 5px; margin-bottom: 20px; }
        .metrics-grid { display: grid; grid-template-columns: repeat(auto-fit, minmax(300px, 1fr)); gap: 20px; margin-bottom: 20px; }
        .metric-card { background: white; padding: 20px; border-radius: 5px; box-shadow: 0 2px 5px rgba(0,0,0,0.1); }
        .metric-value { font-size: 2em; font-weight: bold; color: #3498db; }
        .metric-label { color: #7f8c8d; margin-bottom: 10px; }
        .alerts { background: #e74c3c; color: white; padding: 15px; border-radius: 5px; margin-bottom: 20px; }
        .charts-container { display: grid; grid-template-columns: 1fr 1fr; gap: 20px; margin-bottom: 20px; }
        .chart-container { background: white; padding: 20px; border-radius: 5px; box-shadow: 0 2px 5px rgba(0,0,0,0.1); }
        .chart-full { grid-column: 1 / -1; }
        .service-list { background: white; padding: 20px; border-radius: 5px; }
        .service-item { padding: 10px; border-bottom: 1px solid #ecf0f1; }
        .refresh-btn { background: #3498db; color: white; padding: 10px 20px; border: none; border-radius: 5px; cursor: pointer; margin-right: 10px; }
        .refresh-btn:hover { background: #2980b9; }
        .chart-canvas { max-height: 300px; }
        .chart-title { margin-bottom: 15px; font-weight: bold; color: #2c3e50; }
    </style>
</head>
<body>
    <div class="container">
        <div class="header">
            <h1>Real-Time Analytics Dashboard</h1>
            <button class="refresh-btn" onclick="refreshData()">Refresh Data</button>
            <button class="refresh-btn" onclick="toggleAutoRefresh()" id="auto-refresh-btn">Auto Refresh: ON</button>
        </div>
        
        <div id="alerts"></div>
        
        <div class="metrics-grid">
            <div class="metric-card">
                <div class="metric-label">Total Services</div>
                <div class="metric-value" id="total-services">-</div>
            </div>
            <div class="metric-card">
                <div class="metric-label">Active Alerts</div>
                <div class="metric-value" id="active-alerts">-</div>
            </div>
            <div class="metric-card">
                <div class="metric-label">Average CPU Usage</div>
                <div class="metric-value" id="avg-cpu">-</div>
            </div>
            <div class="metric-card">
                <div class="metric-label">Total Requests</div>
                <div class="metric-value" id="total-requests">-</div>
            </div>
        </div>
        
        <div class="charts-container">
            <div class="chart-container chart-full">
                <div class="chart-title">CPU Usage Over Time</div>
                <canvas id="cpu-chart" class="chart-canvas"></canvas>
            </div>
            <div class="chart-container">
                <div class="chart-title">Memory Usage</div>
                <canvas id="memory-chart" class="chart-canvas"></canvas>
            </div>
            <div class="chart-container">
                <div class="chart-title">Response Time</div>
                <canvas id="response-time-chart" class="chart-canvas"></canvas>
            </div>
        </div>
        
        <div class="service-list">
            <h3>Service Status</h3>
            <div id="services"></div>
        </div>
    </div>
    
    <script>
        let cpuChart = null;
        let memoryChart = null;
        let responseTimeChart = null;
        let autoRefresh = true;
        let refreshInterval = null;
        
        // Chart.js configuration
        const chartConfig = {
            type: 'line',
            options: {
                responsive: true,
                maintainAspectRatio: false,
                scales: {
                    y: {
                        beginAtZero: true
                    }
                },
                plugins: {
                    legend: {
                        position: 'top',
                    }
                }
            }
        };
        
        function initializeCharts() {
            // CPU Chart
            const cpuCtx = document.getElementById('cpu-chart').getContext('2d');
            cpuChart = new Chart(cpuCtx, {
                ...chartConfig,
                data: { labels: [], datasets: [] },
                options: {
                    ...chartConfig.options,
                    scales: {
                        ...chartConfig.options.scales,
                        y: {
                            beginAtZero: true,
                            max: 100,
                            ticks: {
                                callback: function(value) {
                                    return value + '%';
                                }
                            }
                        }
                    }
                }
            });
            
            // Memory Chart
            const memoryCtx = document.getElementById('memory-chart').getContext('2d');
            memoryChart = new Chart(memoryCtx, {
                ...chartConfig,
                data: { labels: [], datasets: [] },
                options: {
                    ...chartConfig.options,
                    scales: {
                        ...chartConfig.options.scales,
                        y: {
                            beginAtZero: true,
                            ticks: {
                                callback: function(value) {
                                    return value + ' MB';
                                }
                            }
                        }
                    }
                }
            });
            
            // Response Time Chart
            const responseTimeCtx = document.getElementById('response-time-chart').getContext('2d');
            responseTimeChart = new Chart(responseTimeCtx, {
                ...chartConfig,
                data: { labels: [], datasets: [] },
                options: {
                    ...chartConfig.options,
                    scales: {
                        ...chartConfig.options.scales,
                        y: {
                            beginAtZero: true,
                            ticks: {
                                callback: function(value) {
                                    return value + ' ms';
                                }
                            }
                        }
                    }
                }
            });
        }
        
        function updateChart(chart, data) {
            chart.data = data;
            chart.update('none'); // No animation for real-time updates
        }
        
        function refreshData() {
            // Fetch current metrics
            fetch('/api/metrics')
                .then(response => response.json())
                .then(data => {
                    updateMetrics(data);
                })
                .catch(error => console.error('Error fetching metrics:', error));
            
            // Fetch charts data
            Promise.all([
                fetch('/api/charts/cpu').then(r => r.json()),
                fetch('/api/charts/memory').then(r => r.json()),
                fetch('/api/charts/response-time').then(r => r.json())
            ]).then(([cpuData, memoryData, responseTimeData]) => {
                updateChart(cpuChart, cpuData);
                updateChart(memoryChart, memoryData);
                updateChart(responseTimeChart, responseTimeData);
            }).catch(error => console.error('Error fetching chart data:', error));
        }
        
        function updateMetrics(data) {
            // Update basic metrics
            const services = Object.keys(data.metrics_aggregates || {});
            document.getElementById('total-services').textContent = services.length;
            document.getElementById('active-alerts').textContent = (data.alerts || []).length;
            
            // Calculate average CPU
            let totalCpu = 0;
            let cpuCount = 0;
            for (const service in data.metrics_aggregates || {}) {
                const cpu = data.metrics_aggregates[service].cpuUsage;
                if (cpu && cpu.avg) {
                    totalCpu += cpu.avg;
                    cpuCount++;
                }
            }
            const avgCpu = cpuCount > 0 ? (totalCpu / cpuCount).toFixed(1) : 0;
            document.getElementById('avg-cpu').textContent = avgCpu + '%';
            
            // Calculate total requests
            let totalRequests = 0;
            for (const service in data.metrics_aggregates || {}) {
                const requests = data.metrics_aggregates[service].requestCount;
                if (requests && requests.latest) {
                    totalRequests += requests.latest;
                }
            }
            document.getElementById('total-requests').textContent = totalRequests;
            
            // Update alerts
            const alertsDiv = document.getElementById('alerts');
            if (data.alerts && data.alerts.length > 0) {
                alertsDiv.innerHTML = '<h3>Active Alerts</h3>' + 
                    data.alerts.map(alert => `<div>${alert.message}</div>`).join('');
                alertsDiv.style.display = 'block';
            } else {
                alertsDiv.style.display = 'none';
            }
            
            // Update services list
            const servicesDiv = document.getElementById('services');
            servicesDiv.innerHTML = services.map(service => {
                const metrics = data.metrics_aggregates[service];
                const cpu = metrics.cpuUsage ? metrics.cpuUsage.latest.toFixed(1) : 'N/A';
                const memory = metrics.memoryUsage ? metrics.memoryUsage.latest.toFixed(0) : 'N/A';
                const responseTime = metrics.responseTime ? metrics.responseTime.latest.toFixed(1) : 'N/A';
                
                return `<div class="service-item">
                    <strong>${service}</strong><br>
                    CPU: ${cpu}% | Memory: ${memory}MB | Response Time: ${responseTime}ms
                </div>`;
            }).join('');
        }
        
        function toggleAutoRefresh() {
            autoRefresh = !autoRefresh;
            const btn = document.getElementById('auto-refresh-btn');
            btn.textContent = `Auto Refresh: ${autoRefresh ? 'ON' : 'OFF'}`;
            
            if (autoRefresh) {
                startAutoRefresh();
            } else {
                stopAutoRefresh();
            }
        }
        
        function startAutoRefresh() {
            if (refreshInterval) clearInterval(refreshInterval);
            refreshInterval = setInterval(refreshData, 5000);
        }
        
        function stopAutoRefresh() {
            if (refreshInterval) {
                clearInterval(refreshInterval);
                refreshInterval = null;
            }
        }
        
        // Initialize everything
        document.addEventListener('DOMContentLoaded', function() {
            initializeCharts();
            refreshData();
            startAutoRefresh();
        });
        
        // Cleanup on page unload
        window.addEventListener('beforeunload', function() {
            stopAutoRefresh();
        });
    </script>
</body>
</html>
"""

@app.route('/')
def dashboard():
    """Serve the dashboard"""
    return render_template_string(DASHBOARD_HTML)

if __name__ == '__main__':
    print("Starting Analytics API Server...")
    print("Dashboard available at: http://localhost:5000")
    print("API endpoints:")
    print("  - GET /api/metrics - Current metrics")
    print("  - GET /api/metrics/historical/<hours> - Historical data")
    print("  - GET /api/service/<name> - Service-specific metrics")
    print("  - GET /api/alerts - Recent alerts")
    print("  - GET /api/charts/cpu - CPU usage chart data")
    print("  - GET /api/charts/memory - Memory usage chart data")
    print("  - GET /api/charts/response-time - Response time chart data")
    
    try:
        app.run(host='0.0.0.0', port=5000, debug=False, threaded=True)
    except KeyboardInterrupt:
        analytics.running = False
        print("API Server stopped")

    
# new comment