package com.cloudsecurity.analytics.model;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.nio.charset.CharacterCodingException;
import java.nio.charset.CodingErrorAction;
import java.time.Instant;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.Base64;
import java.util.LinkedHashMap;
import java.util.Map;

public class DlqRecord {

    private static final int MAX_ORIGINAL_BYTES = 131072;

    private static final DateTimeFormatter TIMESTAMP_FORMAT =
            DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss.SSS'Z'")
                    .withZone(ZoneOffset.UTC);

    public String schemaVersion = "1.0";
    public String failedAt;
    public String stage = "flink-analytics";
    public String errorCode;
    public String errorMessage;
    public int attempts = 1;
    public Map<String, Object> original;
    public Map<String, Object> failedBy;

    public DlqRecord() {}

    public static DlqRecord from(KafkaInputRecord record) {
        DlqRecord dlq = new DlqRecord();

        dlq.failedAt = TIMESTAMP_FORMAT.format(Instant.now());
        dlq.errorCode = record.errorCode == null
      ? "PROCESSING_FAILED"
      : record.errorCode;
        dlq.errorMessage = limit(record.error, 1000);

        byte[] raw = record.rawValueBytes == null
                ? new byte[0]
                : record.rawValueBytes;

        boolean truncated = raw.length > MAX_ORIGINAL_BYTES;

        byte[] payload = truncated
                ? java.util.Arrays.copyOf(raw, MAX_ORIGINAL_BYTES)
                : raw;

        String encoding;
        String value;

        if (!truncated && isValidUtf8(payload)) {
            encoding = "utf8";
            value = new String(payload, java.nio.charset.StandardCharsets.UTF_8);
        } else {
            encoding = "base64";
            value = Base64.getEncoder().encodeToString(payload);
        }

        dlq.original = new LinkedHashMap<>();
        dlq.original.put("topic", record.topic);
        dlq.original.put("partition", record.partition);
        dlq.original.put("offset", record.offset);
        dlq.original.put("key", record.key);
        dlq.original.put(
                "timestamp",
                record.kafkaTimestampMillis == null
                        || record.kafkaTimestampMillis < 0
                        ? null
                        : TIMESTAMP_FORMAT.format(
                                Instant.ofEpochMilli(record.kafkaTimestampMillis)
                        )
        );
        dlq.original.put("valueEncoding", encoding);
        dlq.original.put("value", value);
        dlq.original.put("truncated", truncated);

        dlq.failedBy = new LinkedHashMap<>();
        dlq.failedBy.put("name", "flink-analytics");
        dlq.failedBy.put("version", "1.0");

        return dlq;
    }

    private static boolean isValidUtf8(byte[] bytes) {
        try {
            StandardCharsets.UTF_8.newDecoder()
                    .onMalformedInput(CodingErrorAction.REPORT)
                    .onUnmappableCharacter(CodingErrorAction.REPORT)
                    .decode(ByteBuffer.wrap(bytes));
            return true;
        } catch (CharacterCodingException exception) {
            return false;
        }
    }

    private static String limit(String value, int maxLength) {
        if (value == null) {
            return "Unknown processing error";
        }

        return value.length() <= maxLength
                ? value
                : value.substring(0, maxLength);
    }
}
