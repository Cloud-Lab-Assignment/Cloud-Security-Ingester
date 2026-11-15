package com.cloudsecurity.analytics.sink;

import com.cloudsecurity.analytics.AnalyticsConfig;
import com.cloudsecurity.analytics.model.AnalyticsResult;
import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.databind.module.SimpleModule;
import com.fasterxml.jackson.databind.JsonSerializer;
import com.fasterxml.jackson.databind.SerializerProvider;
import org.apache.flink.connector.base.DeliveryGuarantee;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeFormatterBuilder;
import java.time.temporal.ChronoField;

public final class AnalyticsKafkaSink {

    private AnalyticsKafkaSink() {}

    private static final DateTimeFormatter UTC_MILLIS =
            new DateTimeFormatterBuilder()
                    .appendPattern("yyyy-MM-dd'T'HH:mm:ss")
                    .appendFraction(ChronoField.NANO_OF_SECOND, 3, 3, true)
                    .appendLiteral('Z')
                    .toFormatter()
                    .withZone(java.time.ZoneOffset.UTC);

    private static final ObjectMapper MAPPER = createMapper();

    private static ObjectMapper createMapper() {
        SimpleModule module = new SimpleModule();
        module.addSerializer(Instant.class, new JsonSerializer<Instant>() {
            @Override
            public void serialize(
                    Instant value,
                    JsonGenerator generator,
                    SerializerProvider serializers) throws IOException {
                generator.writeString(UTC_MILLIS.format(value));
            }
        });

        return new ObjectMapper()
                .registerModule(module)
                .disable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS);
    }

    public static KafkaSink<AnalyticsResult> create() {
        return KafkaSink.<AnalyticsResult>builder()
                .setBootstrapServers(AnalyticsConfig.BOOTSTRAP_SERVERS)
                .setRecordSerializer(
                        new KafkaRecordSerializationSchema<AnalyticsResult>() {
                            @Override
                            public ProducerRecord<byte[], byte[]> serialize(
                                    AnalyticsResult result,
                                    KafkaSinkContext context,
                                    Long timestamp) {
                                try {
                                    String key = result.metric + "|"
                                            + result.dimension.value;

                                    byte[] keyBytes =
                                            key.getBytes(StandardCharsets.UTF_8);

                                    byte[] valueBytes =
                                            MAPPER.writeValueAsBytes(result);

                                    return new ProducerRecord<>(
                                            AnalyticsConfig.OUTPUT_TOPIC,
                                            keyBytes,
                                            valueBytes
                                    );
                                } catch (Exception e) {
                                    throw new RuntimeException(
                                            "Failed to serialize analytics result",
                                            e
                                    );
                                }
                            }
                        }
                )
                .setDeliveryGuarantee(DeliveryGuarantee.AT_LEAST_ONCE)
                .build();
    }
}
