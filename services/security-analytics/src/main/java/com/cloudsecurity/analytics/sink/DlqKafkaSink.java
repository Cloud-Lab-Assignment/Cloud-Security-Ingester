package com.cloudsecurity.analytics.sink;

import com.cloudsecurity.analytics.AnalyticsConfig;
import com.cloudsecurity.analytics.model.DlqRecord;
import com.cloudsecurity.analytics.model.KafkaInputRecord;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.connector.base.DeliveryGuarantee;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.nio.charset.StandardCharsets;

public final class DlqKafkaSink {

    private DlqKafkaSink() {}

    private static final ObjectMapper MAPPER = new ObjectMapper();

    public static KafkaSink<KafkaInputRecord> create() {
        return KafkaSink.<KafkaInputRecord>builder()
                .setBootstrapServers(AnalyticsConfig.BOOTSTRAP_SERVERS)
                .setRecordSerializer(
                        new KafkaRecordSerializationSchema<KafkaInputRecord>() {
                            @Override
                            public ProducerRecord<byte[], byte[]> serialize(
                                    KafkaInputRecord record,
                                    KafkaSinkContext context,
                                    Long timestamp) {
                                try {
                                    DlqRecord dlq = DlqRecord.from(record);

                                    byte[] key = record.key == null
                                            ? null
                                            : record.key.getBytes(StandardCharsets.UTF_8);

                                    byte[] value = MAPPER.writeValueAsBytes(dlq);

                                    return new ProducerRecord<>(
                                            AnalyticsConfig.DLQ_TOPIC,
                                            key,
                                            value
                                    );
                                } catch (Exception e) {
                                    throw new RuntimeException(
                                            "Failed to serialize DLQ record",
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
