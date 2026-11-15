package com.cloudsecurity.analytics.processing;

import com.cloudsecurity.analytics.AnalyticsConfig;
import com.cloudsecurity.analytics.model.NormalizedEvent;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

public class EventDeduplicationFunction
        extends KeyedProcessFunction<String, NormalizedEvent, NormalizedEvent> {

    private transient ValueState<Boolean> seen;

    @Override
    public void open(Configuration parameters) {
        ValueStateDescriptor<Boolean> descriptor =
                new ValueStateDescriptor<>("event-seen", Types.BOOLEAN);

        seen = getRuntimeContext().getState(descriptor);
    }

    @Override
    public void processElement(
            NormalizedEvent event,
            Context context,
            Collector<NormalizedEvent> out) throws Exception {

        if (Boolean.TRUE.equals(seen.value())) {
            return;
        }

        seen.update(true);

        long cleanupTime = event.eventTime
                .plusSeconds(AnalyticsConfig.DEDUP_RETENTION_SECONDS)
                .toEpochMilli();

        context.timerService().registerEventTimeTimer(cleanupTime);

        out.collect(event);
    }

    @Override
    public void onTimer(
            long timestamp,
            OnTimerContext context,
            Collector<NormalizedEvent> out) throws Exception {

        seen.clear();
    }
}
