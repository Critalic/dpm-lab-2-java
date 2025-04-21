package com.kpi.producerstatefulstream.config;

import com.kpi.producerstatefulstream.dto.GenData;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.Branched;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.JoinWindows;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.kstream.StreamJoined;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.annotation.EnableKafka;
import org.springframework.kafka.annotation.EnableKafkaStreams;
import org.springframework.kafka.annotation.KafkaStreamsDefaultConfiguration;
import org.springframework.kafka.config.KafkaStreamsConfiguration;
import org.springframework.kafka.support.serializer.JsonDeserializer;
import org.springframework.kafka.support.serializer.JsonSerializer;

import java.time.Duration;
import java.util.HashMap;
import java.util.Map;

import static org.apache.kafka.streams.StreamsConfig.APPLICATION_ID_CONFIG;
import static org.apache.kafka.streams.StreamsConfig.BOOTSTRAP_SERVERS_CONFIG;
import static org.apache.kafka.streams.StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG;
import static org.apache.kafka.streams.StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG;

@Slf4j
@Configuration
@RequiredArgsConstructor
@EnableKafkaStreams
@EnableKafka
public class SplitStreamConfig {
    @Value("${gen-data.stream.kafka.topic}")
    private String sourceTopic;

    private final static String OUTPUT_TOPIC_TYPE_1 = "weather_forecast.type1";
    private final static String OUTPUT_TOPIC_TYPE_2 = "weather_forecast.type2";
    private final static String OUTPUT_TOPIC_JOINED = "weather_forecast.joined";

    @Bean
    public KStream<String, GenData> processWeatherForecasts(StreamsBuilder builder, Serde<GenData> weatherForecastSerde) {
        KStream<String, GenData> weatherStream = builder.stream(
                sourceTopic,
                Consumed.with(Serdes.String(), weatherForecastSerde)
        );

        // Розгалуження потоку за типом
        KStream<String, GenData>[] branches = weatherStream.branch(
                (key, value) -> value.type() == 0,
                (key, value) -> value.type() == 1
        );
        KStream<String, GenData> stream1 = branches[0];
        stream1.to(OUTPUT_TOPIC_TYPE_1);
        KStream<String, GenData> stream2 = branches[1];
        stream2.to(OUTPUT_TOPIC_TYPE_2);

        // Об’єднання (join) двох потоків
        KStream<String, String> joinedStream = stream1.join(
                stream2,
                (forecast1, forecast2) -> String.format(
                        "City: %s, type 1 Temp: %d; City: %s, type 2 Temp: %d",
                        forecast1.city(),
                        forecast1.temperature(),
                        forecast2.city(),
                        forecast2.temperature()
                ),
                JoinWindows.ofTimeDifferenceWithNoGrace(Duration.ofMinutes(5)), // Вікно об’єднання - 5 хвилин
                StreamJoined.with(Serdes.String(), weatherForecastSerde, weatherForecastSerde)
        );

        // Запис результатів об’єднання в топік
        joinedStream.to(OUTPUT_TOPIC_JOINED, Produced.with(Serdes.String(), Serdes.String()));
        return weatherStream;
    }

    @Bean
    public Serde<GenData> weatherForecastSerde() {
        return Serdes.serdeFrom(new JsonSerializer<>(), new JsonDeserializer<>(GenData.class));
    }
}
