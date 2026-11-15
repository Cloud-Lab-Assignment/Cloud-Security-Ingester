package com.cloudsecurity.analytics.source;

import com.cloudsecurity.analytics.model.KafkaInputRecord;
import com.cloudsecurity.analytics.model.NormalizedEvent;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.JsonDeserializer;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.module.SimpleModule;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.connector.kafka.source.reader.deserializer.KafkaRecordDeserializationSchema;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.flink.util.Collector;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.time.format.ResolverStyle;

public class NormalizedEventDeserializer
        implements KafkaRecordDeserializationSchema<KafkaInputRecord> {

    private static final DateTimeFormatter UTC_MILLIS =
            DateTimeFormatter
                    .ofPattern("uuuu-MM-dd'T'HH:mm:ss.SSS'Z'")
                    .withResolverStyle(ResolverStyle.STRICT);

    private static final ObjectMapper MAPPER = createMapper();

    private static ObjectMapper createMapper() {
        SimpleModule timestampModule = new SimpleModule();

        timestampModule.addDeserializer(
                Instant.class,
                new JsonDeserializer<Instant>() {
                    @Override
                    public Instant deserialize(
                            JsonParser parser,
                            DeserializationContext context
                    ) throws IOException {

                        String value = parser.getValueAsString();

                        try {
                            LocalDateTime dateTime =
                                    LocalDateTime.parse(value, UTC_MILLIS);

                            return dateTime.toInstant(ZoneOffset.UTC);

                        } catch (Exception exception) {
                            throw new IOException(
                                    "Invalid timestamp: expected UTC format "
                                            + "yyyy-MM-dd'T'HH:mm:ss.SSS'Z'",
                                    exception
                            );
                        }
                    }
                }
        );

        return new ObjectMapper()
                .registerModule(new JavaTimeModule())
                .registerModule(timestampModule)
                .disable(DeserializationFeature.ADJUST_DATES_TO_CONTEXT_TIME_ZONE)
                .disable(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES)
                .enable(DeserializationFeature.FAIL_ON_TRAILING_TOKENS);
    }

    @Override
    public void deserialize(
            ConsumerRecord<byte[], byte[]> record,
            Collector<KafkaInputRecord> out
    ) throws IOException {

        KafkaInputRecord result = new KafkaInputRecord();

        result.topic = record.topic();
        result.partition = record.partition();
        result.offset = record.offset();
        result.kafkaTimestampMillis = record.timestamp();

        result.key = record.key() == null
                ? null
                : new String(record.key(), StandardCharsets.UTF_8);

        byte[] value = record.value();

        if (value == null) {
            result.error = "Kafka record has a null value";
            result.errorCode = "INVALID_JSON";
            out.collect(result);
            return;
        }

        result.rawValueBytes = value.clone();
        result.rawValue = new String(value, StandardCharsets.UTF_8);

        try {
            result.event = MAPPER.readValue(value, NormalizedEvent.class);

            if (result.event == null) {
                throw new IOException("JSON value is null");
            }

        } catch (Exception exception) {
            result.event = null;
            result.error = exception.getMessage() == null
                    ? "Unable to deserialize normalized event"
                    : exception.getMessage();
            result.errorCode = "INVALID_JSON";
        }

        out.collect(result);
    }

    @Override
    public TypeInformation<KafkaInputRecord> getProducedType() {
        return TypeInformation.of(KafkaInputRecord.class);
    }
}
