package com.cloudsecurity.analytics.processing;

import com.cloudsecurity.analytics.model.KafkaInputRecord;
import org.apache.flink.util.OutputTag;
import org.apache.flink.api.common.typeinfo.TypeInformation;

public final class AnalyticsOutputTags {

    private AnalyticsOutputTags() {}

    public static final OutputTag<KafkaInputRecord> INVALID_RECORDS =
            new OutputTag<KafkaInputRecord>(
                    "invalid-normalized-events",
                    TypeInformation.of(KafkaInputRecord.class)
            ) {};
}
