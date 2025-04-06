package com.kpi.producer.configuration;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.core.ProducerFactory;

import java.util.HashMap;
import java.util.Map;

@Configuration
public class ProducerConfiguration {
    @Value("${spring.kafka.bootstrap-servers}")
    private String bootstrapServers;

    @Bean
    public ObjectMapper objectMapper() {
        return new ObjectMapper();
    }

    // Configuration 1: Basic configuration
    @Bean
    public Map<String, Object> basicProducerConfigs() {
        Map<String, Object> props = new HashMap<>();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        return props;
    }

    // Configuration 2: With high throughput settings
    @Bean
    public Map<String, Object> highThroughputProducerConfigs() {
        Map<String, Object> props = basicProducerConfigs();
        props.put(ProducerConfig.BATCH_SIZE_CONFIG, 32768); // 32KB batch size
        props.put(ProducerConfig.LINGER_MS_CONFIG, 20); // 20ms delay to allow batching
        props.put(ProducerConfig.COMPRESSION_TYPE_CONFIG, "snappy");
        props.put(ProducerConfig.BUFFER_MEMORY_CONFIG, 67108864); // 64MB buffer
        return props;
    }

    // Configuration 3: For low latency scenarios
    @Bean
    public Map<String, Object> lowLatencyProducerConfigs() {
        Map<String, Object> props = basicProducerConfigs();
        props.put(ProducerConfig.LINGER_MS_CONFIG, 0); // No delay
        props.put(ProducerConfig.ACKS_CONFIG, "1"); // Only wait for leader acknowledgement
        props.put(ProducerConfig.BATCH_SIZE_CONFIG, 16384); // Smaller batches
        return props;
    }

    // Default ProducerFactory with basic configuration
    @Bean
    public ProducerFactory<String, String> producerFactory() {
        return new DefaultKafkaProducerFactory<>(basicProducerConfigs());
        // return new DefaultKafkaProducerFactory<>(highThroughputProducerConfigs());
        // return new DefaultKafkaProducerFactory<>(lowLatencyProducerConfigs());
    }

    @Bean
    public KafkaTemplate<String, String> kafkaTemplate() {
        return new KafkaTemplate<>(producerFactory());
    }
}
