package com.cloudsecurity.analytics.model;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;

import java.time.Instant;
import java.util.Map;

@JsonIgnoreProperties(ignoreUnknown = true)
public class NormalizedEvent {

    public String schemaVersion;
    public String eventId;

    public Instant eventTime;
    public Instant receivedAt;
    public Instant normalizedAt;

    public String entityKey;
    public String category;
    public String action;
    public String outcome;

    public Map<String, Object> actor;
    public Map<String, Object> network;
    public Map<String, Object> target;
    public Map<String, Object> source;

    public String message;
    public Map<String, Object> attributes;

    public Map<String, Object> origin;
    public Map<String, Object> normalizedBy;

    public String getSourceIp() {
        return getText(network, "srcIp");
    }

    public String getUserId() {
        String userId = getText(actor, "userId");

        if (userId != null) {
            return userId;
        }

        return getText(actor, "userName");
    }

    public String getTargetId() {
        return getText(target, "resourceId");
    }

    public static String getText(Map<String, Object> map, String field) {
        if (map == null) {
            return null;
        }

        Object value = map.get(field);
        return value instanceof String text && !text.isBlank()
                ? text
                : null;
    }
}
