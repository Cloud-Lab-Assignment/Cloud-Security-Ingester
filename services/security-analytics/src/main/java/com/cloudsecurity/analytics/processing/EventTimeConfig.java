package com.cloudsecurity.analytics.processing;

import com.cloudsecurity.analytics.AnalyticsConfig;
import com.cloudsecurity.analytics.model.NormalizedEvent;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;

import java.time.Duration;

public final class EventTimeConfig {

    private EventTimeConfig() {}

    public static WatermarkStrategy<NormalizedEvent> create() {
        return WatermarkStrategy
                .<NormalizedEvent>forBoundedOutOfOrderness(
                        Duration.ofSeconds(
                                AnalyticsConfig.WATERMARK_DELAY_SECONDS
                        )
                )
                .withTimestampAssigner(
                        (event, timestamp) -> event.eventTime.toEpochMilli()
                )
                .withIdleness(
                        Duration.ofSeconds(
                                AnalyticsConfig.WATERMARK_IDLE_SECONDS
                        )
                );
    }
}
