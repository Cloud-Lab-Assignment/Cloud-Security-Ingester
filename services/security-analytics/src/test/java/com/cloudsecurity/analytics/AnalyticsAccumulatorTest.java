package com.cloudsecurity.analytics;

import com.cloudsecurity.analytics.model.NormalizedEvent;
import com.cloudsecurity.analytics.processing.AnalyticsAccumulator;
import org.junit.jupiter.api.Test;

import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

class AnalyticsAccumulatorTest {

    private NormalizedEvent createEvent(
            String outcome, String userId, String userName, String targetId) {

        NormalizedEvent event = new NormalizedEvent();

        event.outcome = outcome;
        event.actor = Map.of(
                "userId", userId == null ? "" : userId,
                "userName", userName == null ? "" : userName
        );
        event.target = Map.of(
                "resourceId", targetId == null ? "" : targetId
        );

        return event;
    }

    @Test
    void countsEventsAndOutcomes() {
        AnalyticsAccumulator accumulator = new AnalyticsAccumulator();

        accumulator.add(createEvent("success", "u1", null, "t1"));
        accumulator.add(createEvent("failure", "u2", null, "t2"));
        accumulator.add(createEvent("unknown", "u3", null, "t3"));

        assertEquals(3, accumulator.eventCount);
        assertEquals(1, accumulator.successCount);
        assertEquals(1, accumulator.failureCount);
    }

    @Test
    void countsDistinctUsersAndTargets() {
        AnalyticsAccumulator accumulator = new AnalyticsAccumulator();

        accumulator.add(createEvent("success", "u1", null, "t1"));
        accumulator.add(createEvent("failure", "u1", null, "t1"));
        accumulator.add(createEvent("success", "u2", null, "t2"));

        assertEquals(2, accumulator.users.size());
        assertEquals(2, accumulator.targets.size());
    }

    @Test
    void usesUsernameWhenUserIdIsMissing() {
        AnalyticsAccumulator accumulator = new AnalyticsAccumulator();

        accumulator.add(createEvent("success", null, "alice", "t1"));
        accumulator.add(createEvent("success", null, "alice", "t2"));
        accumulator.add(createEvent("success", null, "bob", "t3"));

        assertEquals(2, accumulator.users.size());
    }

    @Test
    void ignoresMissingUserAndTargetIds() {
        AnalyticsAccumulator accumulator = new AnalyticsAccumulator();

        accumulator.add(createEvent("unknown", null, null, null));

        assertEquals(1, accumulator.eventCount);
        assertTrue(accumulator.users.isEmpty());
        assertTrue(accumulator.targets.isEmpty());
    }
}
