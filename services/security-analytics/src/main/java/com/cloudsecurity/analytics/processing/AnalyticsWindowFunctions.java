package com.cloudsecurity.analytics.processing;

import com.cloudsecurity.analytics.model.AnalyticsResult;
import com.cloudsecurity.analytics.model.NormalizedEvent;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.time.Instant;

public final class AnalyticsWindowFunctions {

    private AnalyticsWindowFunctions() {}

    public static class AccumulatorFunction
            implements AggregateFunction<
                    NormalizedEvent,
                    AnalyticsAccumulator,
                    AnalyticsAccumulator> {

        @Override
        public AnalyticsAccumulator createAccumulator() {
            return new AnalyticsAccumulator();
        }

        @Override
        public AnalyticsAccumulator add(
                NormalizedEvent event,
                AnalyticsAccumulator accumulator) {
            accumulator.add(event);
            return accumulator;
        }

        @Override
        public AnalyticsAccumulator getResult(
                AnalyticsAccumulator accumulator) {
            return accumulator;
        }

        @Override
        public AnalyticsAccumulator merge(
                AnalyticsAccumulator first,
                AnalyticsAccumulator second) {
            first.merge(second);
            return first;
        }
    }

    public static class ResultFunction
            extends ProcessWindowFunction<
                    AnalyticsAccumulator,
                    AnalyticsResult,
                    String,
                    TimeWindow> {

        private final String metric;
        private final String dimensionName;
        private final int windowSizeSeconds;

        public ResultFunction(
                String metric,
                String dimensionName,
                int windowSizeSeconds) {
            this.metric = metric;
            this.dimensionName = dimensionName;
            this.windowSizeSeconds = windowSizeSeconds;
        }

        @Override
        public void process(
                String dimensionValue,
                Context context,
                Iterable<AnalyticsAccumulator> elements,
                Collector<AnalyticsResult> out) {

            AnalyticsAccumulator accumulator = elements.iterator().next();

            if (accumulator.eventCount == 0) {
                return;
            }

            Instant start = Instant.ofEpochMilli(context.window().getStart());
            Instant end = Instant.ofEpochMilli(context.window().getEnd());

            AnalyticsResult result = new AnalyticsResult();

            result.metric = metric;
            result.dimension = new AnalyticsResult.Dimension(
                    dimensionName,
                    dimensionValue
            );

            result.window = new AnalyticsResult.Window(
                    windowSizeSeconds,
                    start,
                    end
            );

            result.aggregates = new AnalyticsResult.Aggregates(
                    accumulator.eventCount,
                    accumulator.failureCount,
                    accumulator.successCount,
                    accumulator.users.size(),
                    accumulator.targets.size()
            );

            String windowStart = java.time.format.DateTimeFormatter
                    .ofPattern("yyyy-MM-dd'T'HH:mm:ss.SSS'Z'")
                    .withZone(java.time.ZoneOffset.UTC)
                    .format(start);

            result.resultId = metric + "|" + dimensionValue
                    + "|" + windowSizeSeconds + "|" + windowStart;

            result.computedAt = Instant.now();

            out.collect(result);
        }
    }
}
