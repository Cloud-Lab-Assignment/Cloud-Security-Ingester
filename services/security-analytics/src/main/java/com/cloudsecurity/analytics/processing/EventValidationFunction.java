package com.cloudsecurity.analytics.processing;

import com.cloudsecurity.analytics.model.KafkaInputRecord;
import com.cloudsecurity.analytics.model.NormalizedEvent;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;

import java.time.Instant;
import java.util.Map;
import java.util.UUID;
import java.util.regex.Pattern;

public class EventValidationFunction
        extends ProcessFunction<KafkaInputRecord, NormalizedEvent> {

    private static final Pattern NAME_PATTERN =
            Pattern.compile("^[a-z][a-z0-9_]*$");

    private static final Pattern VERSION_PATTERN =
            Pattern.compile("^[0-9]+\\.[0-9]+$");

    private static final Pattern UUID_V4_PATTERN =
            Pattern.compile(
                    "^[0-9a-f]{8}-[0-9a-f]{4}-4[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$"
            );

    @Override
    public void processElement(
            KafkaInputRecord record,
            Context context,
            Collector<NormalizedEvent> out) {

        String error = validate(record);

        if (error != null) {
            record.error = error;
            if (record.errorCode == null) {
                record.errorCode = error.startsWith("Unsupported schemaVersion")
                        ? "UNSUPPORTED_SCHEMA_VERSION"
                        : "SCHEMA_VALIDATION_FAILED";
            }
            context.output(AnalyticsOutputTags.INVALID_RECORDS, record);
            return;
        }

        out.collect(record.event);
    }

    String validate(KafkaInputRecord record) {
        if (record == null) {
            return "Null Kafka record";
        }

        if (!record.isValid()) {
            return record.error == null
                    ? "Invalid normalized event"
                    : record.error;
        }

        NormalizedEvent event = record.event;

        if (isBlank(event.schemaVersion)
                || !VERSION_PATTERN.matcher(event.schemaVersion).matches()) {
            return "Invalid schemaVersion";
        }

        if (!event.schemaVersion.startsWith("1.")) {
            return "Unsupported schemaVersion";
        }

        if (isBlank(event.eventId) || !isUuidV4(event.eventId)) {
            return "Invalid eventId: expected lowercase UUID v4";
        }

        if (!validTimestamp(event.eventTime)
                || !validTimestamp(event.receivedAt)
                || !validTimestamp(event.normalizedAt)) {
            return "Missing or invalid timestamp";
        }

        if (isBlank(event.entityKey)) {
            return "Missing entityKey";
        }

        if (!isValidName(event.category)) {
            return "Invalid category";
        }

        if (!isValidName(event.action)) {
            return "Invalid action";
        }

        if (!"success".equals(event.outcome)
                && !"failure".equals(event.outcome)
                && !"unknown".equals(event.outcome)) {
            return "Invalid outcome";
        }

        String actorError = validateActor(event.actor);
        if (actorError != null) return actorError;

        String networkError = validateNetwork(event.network);
        if (networkError != null) return networkError;

        String targetError = validateTarget(event.target);
        if (targetError != null) return targetError;

        String sourceError = validateSource(event.source);
        if (sourceError != null) return sourceError;

        if (event.message == null || event.message.length() > 500) {
            return "Missing message or message exceeds 500 characters";
        }

        if (event.attributes == null) {
            return "Missing attributes";
        }

        for (Object value : event.attributes.values()) {
            if (value != null
                    && !(value instanceof String)
                    && !(value instanceof Number)
                    && !(value instanceof Boolean)) {
                return "Attributes must contain only scalar values";
            }
        }

        String originError = validateOrigin(event.origin);
        if (originError != null) return originError;

        String serviceError = validateService(event.normalizedBy, "normalizedBy");
        if (serviceError != null) return serviceError;

        String expectedKey = NormalizedEvent.getText(event.network, "srcIp");

        if (expectedKey == null) {
            expectedKey = NormalizedEvent.getText(event.actor, "userId");
        }

        if (expectedKey == null) {
            expectedKey = NormalizedEvent.getText(event.source, "name");
        }

        if (expectedKey == null || !event.entityKey.equals(expectedKey)) {
            return "entityKey does not match the architecture contract";
        }

        return null;
    }

    private String validateActor(Map<String, Object> actor) {
        if (actor == null) return "Missing actor";

        if (!hasNullableString(actor, "userId")
                || !hasNullableString(actor, "userName")) {
            return "actor.userId and actor.userName must be strings or null";
        }

        Object userType = actor.get("userType");

        if (!"human".equals(userType)
                && !"service".equals(userType)
                && !"unknown".equals(userType)) {
            return "Invalid actor.userType";
        }

        return null;
    }

    private String validateNetwork(Map<String, Object> network) {
        if (network == null) return "Missing network";

        String[] stringFields = {"srcIp", "dstIp", "protocol"};

        for (String field : stringFields) {
            if (!hasNullableString(network, field)) {
                return "network." + field + " must be a string or null";
            }
        }

        String[] integerFields = {"srcPort", "dstPort"};

        for (String field : integerFields) {
            if (!hasNullableInteger(network, field)) {
                return "network." + field + " must be an integer or null";
            }
        }

        return null;
    }

    private String validateTarget(Map<String, Object> target) {
        if (target == null) return "Missing target";

        if (!hasNullableString(target, "resourceType")
                || !hasNullableString(target, "resourceId")) {
            return "target.resourceType and target.resourceId must be strings or null";
        }

        return null;
    }

    private String validateSource(Map<String, Object> source) {
        if (source == null) return "Missing source";

        Object type = source.get("type");
        Object name = source.get("name");

        if (!(type instanceof String typeText) || !isValidName(typeText)) {
            return "Invalid source.type";
        }

        if (!(name instanceof String nameText) || nameText.isBlank()) {
            return "Invalid source.name";
        }

        String[] optionalFields = {"host", "region", "accountId"};

        for (String field : optionalFields) {
            if (source.containsKey(field) && !hasNullableString(source, field)) {
                return "source." + field + " must be a string or null";
            }
        }

        return null;
    }

    private String validateOrigin(Map<String, Object> origin) {
        if (origin == null) return "Missing origin";

        Object topic = origin.get("topic");

        if (!(topic instanceof String topicText) || topicText.isBlank()) {
            return "Invalid origin.topic";
        }

        if (!hasInteger(origin, "partition") || !hasInteger(origin, "offset")) {
            return "origin.partition and origin.offset must be integers";
        }

        return null;
    }

    private String validateService(Map<String, Object> service, String field) {
        if (service == null) return "Missing " + field;

        Object name = service.get("name");
        Object version = service.get("version");

        if (!(name instanceof String nameText) || nameText.isBlank()) {
            return "Invalid " + field + ".name";
        }

        if (!(version instanceof String versionText) || versionText.isBlank()) {
            return "Invalid " + field + ".version";
        }

        return null;
    }

    private boolean hasNullableString(Map<String, Object> map, String field) {
        if (!map.containsKey(field)) return false;

        Object value = map.get(field);
        return value == null || value instanceof String;
    }

    private boolean hasNullableInteger(Map<String, Object> map, String field) {
        if (!map.containsKey(field)) return false;

        Object value = map.get(field);
        return value == null || isInteger(value);
    }

    private boolean hasInteger(Map<String, Object> map, String field) {
        return map.containsKey(field) && isInteger(map.get(field));
    }

    private boolean isInteger(Object value) {
        return value instanceof Byte
                || value instanceof Short
                || value instanceof Integer
                || value instanceof Long
                || value instanceof java.math.BigInteger;
    }

    private boolean validTimestamp(Instant timestamp) {
        return timestamp != null && timestamp.getNano() % 1_000_000 == 0;
    }

    private boolean isValidName(String value) {
        return value != null && NAME_PATTERN.matcher(value).matches();
    }

    private boolean isUuidV4(String value) {
        if (value == null || !UUID_V4_PATTERN.matcher(value).matches()) {
            return false;
        }

        try {
            return UUID.fromString(value).version() == 4;
        } catch (IllegalArgumentException exception) {
            return false;
        }
    }

    private boolean isBlank(String value) {
        return value == null || value.isBlank();
    }
}
