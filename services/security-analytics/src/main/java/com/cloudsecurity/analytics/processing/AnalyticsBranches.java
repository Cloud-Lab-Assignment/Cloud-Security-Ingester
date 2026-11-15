package com.cloudsecurity.analytics.processing;

import com.cloudsecurity.analytics.model.AnalyticsResult;
import com.cloudsecurity.analytics.model.NormalizedEvent;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;

public final class AnalyticsBranches {

    private AnalyticsBranches() {}

    public static DataStream<AnalyticsResult> build(
            DataStream<NormalizedEvent> events) {

        DataStream<AnalyticsResult> byCategory = events
                .keyBy(event -> event.category)
                .window(TumblingEventTimeWindows.of(Time.seconds(60)))
                .aggregate(
                        new AnalyticsWindowFunctions.AccumulatorFunction(),
                        new AnalyticsWindowFunctions.ResultFunction(
                                "events_per_category",
                                "category",
                                60
                        )
                );

        DataStream<AnalyticsResult> bySourceIp = events
                .filter(event -> event.getSourceIp() != null)
                .keyBy(NormalizedEvent::getSourceIp)
                .window(TumblingEventTimeWindows.of(Time.seconds(60)))
                .aggregate(
                        new AnalyticsWindowFunctions.AccumulatorFunction(),
                        new AnalyticsWindowFunctions.ResultFunction(
                                "events_per_source_ip",
                                "srcIp",
                                60
                        )
                );

        DataStream<AnalyticsResult> authBySourceIp = events
                .filter(event ->
                        "authentication".equals(event.category)
                                && event.getSourceIp() != null
                )
                .keyBy(NormalizedEvent::getSourceIp)
                .window(TumblingEventTimeWindows.of(Time.seconds(300)))
                .aggregate(
                        new AnalyticsWindowFunctions.AccumulatorFunction(),
                        new AnalyticsWindowFunctions.ResultFunction(
                                "auth_activity_per_source_ip",
                                "srcIp",
                                300
                        )
                );

        return byCategory
                .union(bySourceIp)
                .union(authBySourceIp);
    }
}
