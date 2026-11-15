package com.cloudsecurity.analytics.model;

import com.cloudsecurity.analytics.model.DlqRecord;
import com.cloudsecurity.analytics.model.KafkaInputRecord;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

class DlqRecordTest {

    private KafkaInputRecord createRecord() {
        KafkaInputRecord record = new KafkaInputRecord();
        record.topic = "normalized-events";
        record.partition = 2;
        record.offset = 15L;
        record.key = "192.168.1.10";
        record.kafkaTimestampMillis = 1735689600000L;
        record.rawValueBytes = "{\"eventId\":\"test\"}"
                .getBytes(StandardCharsets.UTF_8);
        record.error = "Test processing error";
        return record;
    }

    @Test
    void preservesExplicitErrorCode() {
        KafkaInputRecord record = createRecord();
        record.errorCode = "SCHEMA_VALIDATION_FAILED";

        DlqRecord dlq = DlqRecord.from(record);

        assertEquals("SCHEMA_VALIDATION_FAILED", dlq.errorCode);
    }

    @Test
    void defaultsMissingErrorCode() {
        KafkaInputRecord record = createRecord();

        DlqRecord dlq = DlqRecord.from(record);

        assertEquals("PROCESSING_FAILED", dlq.errorCode);
    }

    @Test
    void preservesOriginalKafkaMetadataAndUtf8Value() {
        KafkaInputRecord record = createRecord();

        DlqRecord dlq = DlqRecord.from(record);
        Map<String, Object> original = dlq.original;

        assertEquals("normalized-events", original.get("topic"));
        assertEquals(2, original.get("partition"));
        assertEquals(15L, original.get("offset"));
        assertEquals("192.168.1.10", original.get("key"));
        assertEquals("utf8", original.get("valueEncoding"));
        assertEquals("{\"eventId\":\"test\"}", original.get("value"));
        assertEquals(false, original.get("truncated"));
    }

    @Test
    void encodesInvalidUtf8AsBase64() {
        KafkaInputRecord record = createRecord();
        record.rawValueBytes = new byte[]{(byte) 0xC3, (byte) 0x28};

        DlqRecord dlq = DlqRecord.from(record);

        assertEquals("base64", dlq.original.get("valueEncoding"));
        assertEquals("wyg=", dlq.original.get("value"));
    }

    @Test
    void truncatesPayloadsLargerThanLimit() {
        KafkaInputRecord record = createRecord();
        record.rawValueBytes = new byte[131073];

        DlqRecord dlq = DlqRecord.from(record);

        assertEquals("base64", dlq.original.get("valueEncoding"));
        assertEquals(true, dlq.original.get("truncated"));
        assertEquals(
                131072,
                java.util.Base64.getDecoder()
                        .decode((String) dlq.original.get("value")).length
        );
    }

    @Test
    void limitsErrorMessageToOneThousandCharacters() {
        KafkaInputRecord record = createRecord();
        record.error = "x".repeat(1200);

        DlqRecord dlq = DlqRecord.from(record);

        assertEquals(1000, dlq.errorMessage.length());
    }
}
