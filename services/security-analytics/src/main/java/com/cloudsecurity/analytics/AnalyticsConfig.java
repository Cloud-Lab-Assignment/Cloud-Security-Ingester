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

    // Confluent Cloud / any SASL_SSL cluster: set both to enable SASL PLAIN over TLS.
    // Unset = plaintext (local Docker Compose).
    public static final String KAFKA_USERNAME = getEnv("KAFKA_USERNAME", "");

    public static final String KAFKA_PASSWORD = getEnv("KAFKA_PASSWORD", "");

    /** Client security settings shared by the source and both sinks. */
    public static java.util.Properties kafkaSecurityProperties() {
        java.util.Properties props = new java.util.Properties();
        if (!KAFKA_USERNAME.isEmpty() && !KAFKA_PASSWORD.isEmpty()) {
            props.setProperty("security.protocol", "SASL_SSL");
            props.setProperty("sasl.mechanism", "PLAIN");
            props.setProperty(
                    "sasl.jaas.config",
                    "org.apache.kafka.common.security.plain.PlainLoginModule required "
                            + "username=\"" + KAFKA_USERNAME + "\" "
                            + "password=\"" + KAFKA_PASSWORD + "\";");
        }
        return props;
    }

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
