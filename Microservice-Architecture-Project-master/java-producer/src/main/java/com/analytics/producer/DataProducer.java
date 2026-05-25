package com.analytics.producer;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.time.Instant;
import java.util.Properties;
import java.util.Random;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

public class DataProducer {
    private static final String BOOTSTRAP_SERVERS = "localhost:9092";
    private static final String LOG_TOPIC = "log-events";
    private static final String METRICS_TOPIC = "metrics-events";
    
    private final KafkaProducer<String, String> producer;
    private final ObjectMapper objectMapper;
    private final Random random;
    private final ScheduledExecutorService scheduler;
    
    public DataProducer() {
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.ACKS_CONFIG, "1");
        props.put(ProducerConfig.BATCH_SIZE_CONFIG, 16384);
        props.put(ProducerConfig.LINGER_MS_CONFIG, 10);
        
        this.producer = new KafkaProducer<>(props);
        this.objectMapper = new ObjectMapper();
        this.random = new Random();
        this.scheduler = Executors.newScheduledThreadPool(2);
    }
    
    public void startProducing() {
        // Produce log events every 100ms
        scheduler.scheduleAtFixedRate(this::produceLogEvent, 0, 100, TimeUnit.MILLISECONDS);
        
        // Produce metrics events every 1 second
        scheduler.scheduleAtFixedRate(this::produceMetricsEvent, 0, 1000, TimeUnit.MILLISECONDS);
        
        System.out.println("Data producer started. Press Ctrl+C to stop.");
    }
    
    private void produceLogEvent() {
        try {
            LogEvent logEvent = new LogEvent(
                System.currentTimeMillis(),
                generateLogLevel(),
                generateService(),
                generateLogMessage(),
                generateUserId()
            );
            
            String json = objectMapper.writeValueAsString(logEvent);
            ProducerRecord<String, String> record = new ProducerRecord<>(LOG_TOPIC, logEvent.service, json);
            
            producer.send(record, (metadata, exception) -> {
                if (exception != null) {
                    System.err.println("Error sending log event: " + exception.getMessage());
                }
            });
        } catch (Exception e) {
            System.err.println("Error producing log event: " + e.getMessage());
        }
    }
    
    private void produceMetricsEvent() {
        try {
            MetricsEvent metricsEvent = new MetricsEvent(
                System.currentTimeMillis(),
                generateService(),
                random.nextDouble() * 100, // CPU usage
                random.nextDouble() * 8000, // Memory MB
                random.nextInt(1000), // Request count
                random.nextDouble() * 500 // Response time ms
            );
            
            String json = objectMapper.writeValueAsString(metricsEvent);
            ProducerRecord<String, String> record = new ProducerRecord<>(METRICS_TOPIC, metricsEvent.service, json);
            
            producer.send(record, (metadata, exception) -> {
                if (exception != null) {
                    System.err.println("Error sending metrics event: " + exception.getMessage());
                }
            });
        } catch (Exception e) {
            System.err.println("Error producing metrics event: " + e.getMessage());
        }
    }
    
    private String generateLogLevel() {
        String[] levels = {"INFO", "WARN", "ERROR", "DEBUG"};
        return levels[random.nextInt(levels.length)];
    }
    
    private String generateService() {
        String[] services = {"user-service", "order-service", "payment-service", "inventory-service", "notification-service"};
        return services[random.nextInt(services.length)];
    }
    
    private String generateLogMessage() {
        String[] messages = {
            "Request processed successfully",
            "Database connection established",
            "Cache miss occurred",
            "External API call failed",
            "User authentication successful",
            "Rate limit exceeded",
            "Request timeout",
            "Invalid input received"
        };
        return messages[random.nextInt(messages.length)];
    }
    
    private String generateUserId() {
        return "user_" + (1000 + random.nextInt(9000));
    }
    
    public void shutdown() {
        scheduler.shutdown();
        producer.close();
    }
    
    public static void main(String[] args) {
        DataProducer producer = new DataProducer();
        
        Runtime.getRuntime().addShutdownHook(new Thread(producer::shutdown));
        
        producer.startProducing();
        
        // Keep the application running
        try {
            Thread.currentThread().join();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }
    
    // Data classes
    public static class LogEvent {
        public long timestamp;
        public String level;
        public String service;
        public String message;
        public String userId;
        
        public LogEvent() {}
        
        public LogEvent(long timestamp, String level, String service, String message, String userId) {
            this.timestamp = timestamp;
            this.level = level;
            this.service = service;
            this.message = message;
            this.userId = userId;
        }
    }
    
    public static class MetricsEvent {
        public long timestamp;
        public String service;
        public double cpuUsage;
        public double memoryUsage;
        public int requestCount;
        public double responseTime;
        
        public MetricsEvent() {}
        
        public MetricsEvent(long timestamp, String service, double cpuUsage, double memoryUsage, int requestCount, double responseTime) {
            this.timestamp = timestamp;
            this.service = service;
            this.cpuUsage = cpuUsage;
            this.memoryUsage = memoryUsage;
            this.requestCount = requestCount;
            this.responseTime = responseTime;
        }
    }
}