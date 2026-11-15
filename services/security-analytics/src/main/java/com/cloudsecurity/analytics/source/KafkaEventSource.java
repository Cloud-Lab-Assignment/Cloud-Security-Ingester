package com.cloudsecurity.analytics.source;

import com.cloudsecurity.analytics.AnalyticsConfig;
import com.cloudsecurity.analytics.model.KafkaInputRecord;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.kafka.clients.consumer.OffsetResetStrategy;

public final class KafkaEventSource {

    private KafkaEventSource() {}

    public static KafkaSource<KafkaInputRecord> create() {
        return KafkaSource.<KafkaInputRecord>builder()
                .setBootstrapServers(AnalyticsConfig.BOOTSTRAP_SERVERS)
                .setTopics(AnalyticsConfig.INPUT_TOPIC)
                .setGroupId(AnalyticsConfig.GROUP_ID)
                .setStartingOffsets(
                        OffsetsInitializer.committedOffsets(
                                OffsetResetStrategy.EARLIEST
                        )
                )
                .setDeserializer(new NormalizedEventDeserializer())
                .build();
    }
}
