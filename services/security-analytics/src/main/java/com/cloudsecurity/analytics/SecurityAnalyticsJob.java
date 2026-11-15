package com.cloudsecurity.analytics;

import com.cloudsecurity.analytics.model.AnalyticsResult;
import com.cloudsecurity.analytics.model.KafkaInputRecord;
import com.cloudsecurity.analytics.model.NormalizedEvent;
import com.cloudsecurity.analytics.processing.AnalyticsBranches;
import com.cloudsecurity.analytics.processing.AnalyticsOutputTags;
import com.cloudsecurity.analytics.processing.EventDeduplicationFunction;
import com.cloudsecurity.analytics.processing.EventTimeConfig;
import com.cloudsecurity.analytics.processing.EventValidationFunction;
import com.cloudsecurity.analytics.sink.AnalyticsKafkaSink;
import com.cloudsecurity.analytics.sink.DlqKafkaSink;
import com.cloudsecurity.analytics.source.KafkaEventSource;

import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;

import java.time.Duration;

public class SecurityAnalyticsJob {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env =
                StreamExecutionEnvironment.getExecutionEnvironment();

        env.setRuntimeMode(RuntimeExecutionMode.STREAMING);
        env.setParallelism(AnalyticsConfig.PARALLELISM);

        env.enableCheckpointing(
                AnalyticsConfig.CHECKPOINT_INTERVAL_MS,
                CheckpointingMode.AT_LEAST_ONCE
        );

        env.getCheckpointConfig().setCheckpointStorage(
                AnalyticsConfig.CHECKPOINT_URI
        );

        env.getCheckpointConfig().setCheckpointTimeout(
                Duration.ofMinutes(2).toMillis()
        );

        env.setRestartStrategy(
                RestartStrategies.fixedDelayRestart(
                        AnalyticsConfig.RESTART_ATTEMPTS,
                        Duration.ofSeconds(
                                AnalyticsConfig.RESTART_DELAY_SECONDS
                        )
                )
        );

        DataStream<KafkaInputRecord> input = env
                .fromSource(
                        KafkaEventSource.create(),
                        org.apache.flink.api.common.eventtime.WatermarkStrategy.noWatermarks(),
                        "normalized-events-source"
                );

        SingleOutputStreamOperator<NormalizedEvent> validated = input
                .process(new EventValidationFunction())
                .name("validate-normalized-events");

        validated
                .getSideOutput(AnalyticsOutputTags.INVALID_RECORDS)
                .sinkTo(DlqKafkaSink.create())
                .name("normalized-events-dlq-sink");

        DataStream<NormalizedEvent> timedEvents = validated
                .assignTimestampsAndWatermarks(EventTimeConfig.create())
                .name("event-time-and-watermarks");

        DataStream<NormalizedEvent> deduplicated = timedEvents
                .keyBy(event -> event.eventId)
                .process(new EventDeduplicationFunction())
                .name("deduplicate-events");

        DataStream<AnalyticsResult> results =
                AnalyticsBranches.build(deduplicated);

        results
                .sinkTo(AnalyticsKafkaSink.create())
                .name("analytics-results-sink");

        env.execute("Cloud Security Analytics");
    }
}
