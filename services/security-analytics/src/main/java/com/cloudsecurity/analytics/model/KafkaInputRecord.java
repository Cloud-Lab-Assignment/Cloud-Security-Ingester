package com.cloudsecurity.analytics.model;

public class KafkaInputRecord {

    public String topic;
    public int partition;
    public long offset;
    public String key;
    public Long kafkaTimestampMillis;

    public byte[] rawValueBytes;
    public String rawValue;

    public NormalizedEvent event;
    public String error;
    public String errorCode;

    public KafkaInputRecord() {}

    public boolean isValid() {
        return event != null && error == null;
    }
}
