package com.cloudsecurity.analytics;

public final class AnalyticsConfig {

    private AnalyticsConfig() {}

    public static final String BOOTSTRAP_SERVERS =
            getEnv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092");

    public static final String INPUT_TOPIC =
            getEnv("INPUT_TOPIC", "normalized-events");

    public static final String OUTPUT_TOPIC =
            getEnv("OUTPUT_TOPIC", "results");

    public static final String DLQ_TOPIC =
            getEnv("DLQ_TOPIC", "normalized-events-dlq");

    public static final String GROUP_ID =
            getEnv("KAFKA_GROUP_ID", "flink-analytics-cg");

    public static final String CHECKPOINT_URI =
            getEnv("CHECKPOINT_URI", "file:///tmp/flink-checkpoints");

    public static final int PARALLELISM =
            Integer.parseInt(getEnv("FLINK_PARALLELISM", "3"));

    public static final long CHECKPOINT_INTERVAL_MS = 60_000L;

    public static final int RESTART_ATTEMPTS = 3;

    public static final int RESTART_DELAY_SECONDS = 10;

    public static final long WATERMARK_DELAY_SECONDS = 30L;

    public static final long WATERMARK_IDLE_SECONDS = 60L;

    public static final long DEDUP_RETENTION_SECONDS = 360L;

    private static String getEnv(String name, String defaultValue) {
        String value = System.getenv(name);
        return value == null || value.isBlank() ? defaultValue : value;
    }
}
