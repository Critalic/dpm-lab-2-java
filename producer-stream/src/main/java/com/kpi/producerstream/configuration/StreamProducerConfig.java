package com.kpi.producerstream.configuration;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.kpi.producerstream.dto.KafkaMessageForecast;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.Branched;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.annotation.EnableKafka;
import org.springframework.kafka.annotation.EnableKafkaStreams;
import org.springframework.kafka.annotation.KafkaStreamsDefaultConfiguration;
import org.springframework.kafka.config.KafkaStreamsConfiguration;
import org.springframework.kafka.support.serializer.JsonDeserializer;
import org.springframework.kafka.support.serializer.JsonSerializer;

import java.util.HashMap;
import java.util.Map;

import static org.apache.kafka.streams.StreamsConfig.APPLICATION_ID_CONFIG;
import static org.apache.kafka.streams.StreamsConfig.BOOTSTRAP_SERVERS_CONFIG;
import static org.apache.kafka.streams.StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG;
import static org.apache.kafka.streams.StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG;

@Slf4j
@Configuration
@EnableKafka
@EnableKafkaStreams
public class StreamProducerConfig {
    @Value(value = "${spring.kafka.bootstrap-servers}")
    private String bootstrapAddress;
    @Value(value = "${producer.stream.kafka.topic}")
    private String topic;

    private static final String OUTPUT_TOPIC_12_HOURS = "ny-weather-forecast-12h";
    private static final String OUTPUT_TOPIC_24_HOURS = "ny-weather-forecast-24h";
    private static final String OUTPUT_TOPIC_36_HOURS = "ny-weather-forecast-36h";
    private static final String OUTPUT_TOPIC_48_HOURS = "ny-weather-forecast-48h";
    private static final String OUTPUT_TOPIC_UNKNOWN = "ny-weather-forecast-unknown";

    @Bean(name = KafkaStreamsDefaultConfiguration.DEFAULT_STREAMS_CONFIG_BEAN_NAME)
    KafkaStreamsConfiguration kStreamsConfig() {
        Map<String, Object> props = new HashMap<>();
        props.put(APPLICATION_ID_CONFIG, "streams-app");
        props.put(BOOTSTRAP_SERVERS_CONFIG, bootstrapAddress);
        props.put(DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        props.put(DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());

        return new KafkaStreamsConfiguration(props);
    }

    @Bean
    public KStream<String, KafkaMessageForecast> configureStreams(StreamsBuilder builder, Serde<KafkaMessageForecast> forecastDtoSerde) {
        KStream<String, KafkaMessageForecast> weatherStream = builder.stream(
                topic,
                Consumed.with(Serdes.String(), forecastDtoSerde)
        );

        weatherStream
                .filter((key, value) -> "NY".equalsIgnoreCase(value.payload().forecastDto().state()))
                .split()
                .branch((key, value) -> value.payload().forecastDto().forecastHoursBefore() == 12,
                        Branched.withConsumer(ks -> ks.to(OUTPUT_TOPIC_12_HOURS)))
                .branch((key, value) -> value.payload().forecastDto().forecastHoursBefore() == 24,
                        Branched.withConsumer(ks -> ks.to(OUTPUT_TOPIC_24_HOURS)))
                .branch((key, value) -> value.payload().forecastDto().forecastHoursBefore() == 36,
                        Branched.withConsumer(ks -> ks.to(OUTPUT_TOPIC_36_HOURS)))
                .branch((key, value) -> value.payload().forecastDto().forecastHoursBefore() == 48,
                        Branched.withConsumer(ks -> ks.to(OUTPUT_TOPIC_48_HOURS)))
                .defaultBranch(Branched.withConsumer(ks -> ks.to(OUTPUT_TOPIC_UNKNOWN)));
        return weatherStream;
    }

    @Bean
    public Serde<KafkaMessageForecast> forecastDtoSerde() {
        return Serdes.serdeFrom(new JsonSerializer<>(), new JsonDeserializer<>(KafkaMessageForecast.class));
    }
}
