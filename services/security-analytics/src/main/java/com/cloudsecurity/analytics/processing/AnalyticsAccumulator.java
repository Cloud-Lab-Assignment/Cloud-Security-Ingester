package com.cloudsecurity.analytics.processing;

import com.cloudsecurity.analytics.model.NormalizedEvent;

import java.util.HashSet;
import java.util.Set;

public class AnalyticsAccumulator {

    public long eventCount;
    public long failureCount;
    public long successCount;

    public Set<String> users = new HashSet<>();
    public Set<String> targets = new HashSet<>();

    public void add(NormalizedEvent event) {
        eventCount++;

        if ("failure".equals(event.outcome)) {
            failureCount++;
        } else if ("success".equals(event.outcome)) {
            successCount++;
        }

        String userId = event.getUserId();
        if (userId != null) {
            users.add(userId);
        }

        String targetId = event.getTargetId();
        if (targetId != null) {
            targets.add(targetId);
        }
    }

    public void merge(AnalyticsAccumulator other) {
        eventCount += other.eventCount;
        failureCount += other.failureCount;
        successCount += other.successCount;

        users.addAll(other.users);
        targets.addAll(other.targets);
    }
}
