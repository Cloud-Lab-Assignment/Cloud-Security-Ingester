package com.cloudsecurity.analytics.model;

import java.time.Instant;

public class AnalyticsResult {

    public String schemaVersion = "1.0";
    public String resultId;
    public String metric;
    public Dimension dimension;
    public Window window;
    public Aggregates aggregates;
    public Instant computedAt;
    public String producedBy = "security-analytics";

    public static class Dimension {
        public String name;
        public String value;

        public Dimension() {}

        public Dimension(String name, String value) {
            this.name = name;
            this.value = value;
        }
    }

    public static class Window {
        public String type = "tumbling";
        public int sizeSeconds;
        public Instant start;
        public Instant end;

        public Window() {}

        public Window(int sizeSeconds, Instant start, Instant end) {
            this.sizeSeconds = sizeSeconds;
            this.start = start;
            this.end = end;
        }
    }

    public static class Aggregates {
        public long eventCount;
        public long failureCount;
        public long successCount;
        public long distinctUsers;
        public long distinctTargets;

        public Aggregates() {}

        public Aggregates(
                long eventCount,
                long failureCount,
                long successCount,
                long distinctUsers,
                long distinctTargets) {
            this.eventCount = eventCount;
            this.failureCount = failureCount;
            this.successCount = successCount;
            this.distinctUsers = distinctUsers;
            this.distinctTargets = distinctTargets;
        }
    }
}
