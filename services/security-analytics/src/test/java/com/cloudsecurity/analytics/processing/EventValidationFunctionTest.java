package com.cloudsecurity.analytics.processing;

import com.cloudsecurity.analytics.model.KafkaInputRecord;
import com.cloudsecurity.analytics.model.NormalizedEvent;
import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

class EventValidationFunctionTest {

    private final EventValidationFunction validator = new EventValidationFunction();

    private NormalizedEvent validEvent() {
        NormalizedEvent event = new NormalizedEvent();

        event.schemaVersion = "1.0";
        event.eventId = "550e8400-e29b-41d4-a716-446655440000";
        event.eventTime = Instant.parse("2026-11-09T10:00:00.000Z");
        event.receivedAt = Instant.parse("2026-11-09T10:00:01.000Z");
        event.normalizedAt = Instant.parse("2026-11-09T10:00:02.000Z");
        event.entityKey = "192.168.1.10";
        event.category = "authentication";
        event.action = "login";
        event.outcome = "failure";

        event.actor = new HashMap<>(Map.of(
                "userId", "user-1",
                "userType", "human"
        ));
        event.actor.put("userName", null);

        event.network = new HashMap<>();
        event.network.put("srcIp", "192.168.1.10");
        event.network.put("srcPort", 5000);
        event.network.put("dstIp", null);
        event.network.put("dstPort", null);
        event.network.put("protocol", "tcp");

        event.target = new HashMap<>();
        event.target.put("resourceType", "account");
        event.target.put("resourceId", "account-1");

        event.source = new HashMap<>(Map.of(
                "type", "application",
                "name", "test-app"
        ));

        event.message = "Failed login";
        event.attributes = new HashMap<>();

        event.origin = new HashMap<>(Map.of(
                "topic", "normalized-events",
                "partition", 1,
                "offset", 10L
        ));

        event.normalizedBy = new HashMap<>(Map.of(
                "name", "event-normalizer",
                "version", "1.0"
        ));

        return event;
    }

    private KafkaInputRecord recordWith(NormalizedEvent event) {
        KafkaInputRecord record = new KafkaInputRecord();
        record.event = event;
        return record;
    }

    @Test
    void acceptsValidNormalizedEvent() {
        assertNull(validator.validate(recordWith(validEvent())));
    }

    @Test
    void rejectsUnsupportedSchemaVersion() {
        NormalizedEvent event = validEvent();
        event.schemaVersion = "2.0";

        assertEquals("Unsupported schemaVersion",
                validator.validate(recordWith(event)));
    }

    @Test
    void rejectsInvalidUuid() {
        NormalizedEvent event = validEvent();
        event.eventId = "not-a-uuid";

        assertEquals("Invalid eventId: expected lowercase UUID v4",
                validator.validate(recordWith(event)));
    }

    @Test
    void rejectsSubMillisecondTimestamp() {
        NormalizedEvent event = validEvent();
        event.eventTime = Instant.parse("2026-11-09T10:00:00.000000001Z");

        assertEquals("Missing or invalid timestamp",
                validator.validate(recordWith(event)));
    }

    @Test
    void rejectsIncorrectEntityKey() {
        NormalizedEvent event = validEvent();
        event.entityKey = "different-key";

        assertEquals("entityKey does not match the architecture contract",
                validator.validate(recordWith(event)));
    }

    @Test
    void rejectsDecimalOriginPartition() {
        NormalizedEvent event = validEvent();
        event.origin.put("partition", 1.0);

        assertEquals("origin.partition and origin.offset must be integers",
                validator.validate(recordWith(event)));
    }

    @Test
    void rejectsMissingNetworkField() {
        NormalizedEvent event = validEvent();
        event.network.remove("dstIp");

        assertEquals("network.dstIp must be a string or null",
                validator.validate(recordWith(event)));
    }
}
