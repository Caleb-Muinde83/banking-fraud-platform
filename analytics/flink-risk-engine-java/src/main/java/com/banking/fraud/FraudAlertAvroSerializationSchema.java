package com.banking.fraud;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.formats.avro.registry.confluent.ConfluentRegistryAvroSerializationSchema;

/**
 * CHANGED: previously this wrote raw binary Avro using a schema hardcoded in
 * this class, with no Schema Registry round-trip and no Confluent wire
 * format (magic byte + schema ID) — a standard Confluent Avro deserializer
 * elsewhere in the platform (OpenSearch sink, API, etc.) would not have been
 * able to read it. This now delegates actual serialization to Flink's
 * ConfluentRegistryAvroSerializationSchema, which registers/resolves the
 * schema against the real Schema Registry and writes the standard wire
 * format.
 *
 * NOT independently verified against a live Schema Registry in this pass —
 * confirm the subject name (`schema-registry.subject` in
 * risk-scoring.properties) matches what your registry expects
 * (TopicNameStrategy default is "<topic>-value").
 */
public class FraudAlertAvroSerializationSchema implements SerializationSchema<FraudAlert> {
    private static final String SCHEMA_JSON = "{"
        + "\"type\":\"record\","
        + "\"name\":\"FraudAlert\","
        + "\"namespace\":\"com.banking.fraud\","
        + "\"fields\":["
        + "{\"name\":\"alert_id\",\"type\":[\"null\",\"string\"],\"default\":null},"
        + "{\"name\":\"customer_id\",\"type\":[\"null\",\"string\"],\"default\":null},"
        + "{\"name\":\"account_id\",\"type\":[\"null\",\"string\"],\"default\":null},"
        + "{\"name\":\"event_time\",\"type\":\"long\"},"
        + "{\"name\":\"severity\",\"type\":[\"null\",\"string\"],\"default\":null},"
        + "{\"name\":\"cumulative_score\",\"type\":\"int\"},"
        + "{\"name\":\"threshold\",\"type\":\"int\"},"
        + "{\"name\":\"contributing_indicators\",\"type\":{\"type\":\"array\",\"items\":\"string\"}},"
        + "{\"name\":\"trigger_reason\",\"type\":[\"null\",\"string\"],\"default\":null},"
        + "{\"name\":\"first_seen_ts\",\"type\":\"long\"},"
        + "{\"name\":\"latest_seen_ts\",\"type\":\"long\"},"
        + "{\"name\":\"source_event_ids\",\"type\":{\"type\":\"array\",\"items\":\"string\"}},"
        + "{\"name\":\"action_required\",\"type\":[\"null\",\"string\"],\"default\":null}"
        + "]}";
    private static final Schema SCHEMA = new Schema.Parser().parse(SCHEMA_JSON);

    private final String schemaRegistryUrl;
    private final String subject;
    private transient SerializationSchema<GenericRecord> delegate;

    public FraudAlertAvroSerializationSchema(String schemaRegistryUrl, String subject) {
        this.schemaRegistryUrl = schemaRegistryUrl;
        this.subject = subject;
    }

    @Override
    public void open(InitializationContext context) throws Exception {
        delegate = ConfluentRegistryAvroSerializationSchema.forGeneric(subject, SCHEMA, schemaRegistryUrl);
        delegate.open(context);
    }

    @Override
    public byte[] serialize(FraudAlert alert) {
        GenericRecord record = new GenericData.Record(SCHEMA);
        record.put("alert_id", alert.getAlertId());
        record.put("customer_id", alert.getCustomerId());
        record.put("account_id", alert.getAccountId());
        record.put("event_time", alert.getEventTime());
        record.put("severity", alert.getSeverity());
        record.put("cumulative_score", alert.getCumulativeScore());
        record.put("threshold", alert.getThreshold());
        record.put("contributing_indicators", alert.getContributingIndicators());
        record.put("trigger_reason", alert.getTriggerReason());
        record.put("first_seen_ts", alert.getFirstSeenTs());
        record.put("latest_seen_ts", alert.getLatestSeenTs());
        record.put("source_event_ids", alert.getSourceEventIds());
        record.put("action_required", alert.getActionRequired());
        return delegate.serialize(record);
    }
}
