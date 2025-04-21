package com.kpi.producerstatefulstream.config;

import com.kpi.producerstatefulstream.dto.KafkaMessageForecast;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Materialized;
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
public class StatefulStreamConfig {
    @Value(value = "${spring.kafka.bootstrap-servers}")
    private String bootstrapAddress;
    @Value(value = "${producer.stream.kafka.topic}")
    private String topic;

    @Bean(name = KafkaStreamsDefaultConfiguration.DEFAULT_STREAMS_CONFIG_BEAN_NAME)
    KafkaStreamsConfiguration kStreamsConfig() {
        Map<String, Object> props = new HashMap<>();
        props.put(APPLICATION_ID_CONFIG, "stateful-streams-app");
        props.put(BOOTSTRAP_SERVERS_CONFIG, bootstrapAddress);
        props.put(DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        props.put(DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.Integer().getClass().getName());

        return new KafkaStreamsConfiguration(props);
    }

    @Bean
    public KStream<String, KafkaMessageForecast> configureStreams(StreamsBuilder builder, Serde<KafkaMessageForecast> forecastDtoSerde) {
        KStream<String, KafkaMessageForecast> weatherStream = builder.stream(
                topic,
                Consumed.with(Serdes.String(), forecastDtoSerde)
        );

        // Count forecastHoursBefore == 12
        weatherStream.filter((key, value) -> value.getForecastDto().forecastHoursBefore() == 12)
                .map((key, value) -> new KeyValue<>("count", 1)) // re-key everything with a constant key
                .groupByKey()
                .count(Materialized.with(Serdes.String(), Serdes.Long()))
                .toStream()
                .foreach((key, count) -> System.out.println("Count with forecastHoursBefore == 12: " + count));


        // Find max observedTemp for state == "NY"
        weatherStream.filter((key, value) -> "NY".equals(value.getForecastDto().state()) &&
                        value.getForecastDto().observedTemp() != null)
                .map((key, value) -> new KeyValue<>("NY", value.getForecastDto().observedTemp())) // use state as key
                .groupByKey()
                .reduce(Integer::max, Materialized.with(Serdes.String(), Serdes.Integer()))
                .toStream()
                .foreach((key, maxTemp) -> System.out.println("Max observedTemp in NY: " + maxTemp));


        return weatherStream;
    }

    @Bean
    public Serde<KafkaMessageForecast> forecastDtoSerde() {
        return Serdes.serdeFrom(new JsonSerializer<>(), new JsonDeserializer<>(KafkaMessageForecast.class));
    }
}
