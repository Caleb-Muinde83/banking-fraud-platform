package com.banking.fraud;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.connector.base.DeliveryGuarantee;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.contrib.streaming.state.EmbeddedRocksDBStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.AsyncDataStream;
import org.apache.flink.streaming.api.datastream.BroadcastStream;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.time.Duration;
import java.util.Objects;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

public class RiskScoringJob {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env.enableCheckpointing(15_000L, CheckpointingMode.EXACTLY_ONCE);
        env.getCheckpointConfig().setMinPauseBetweenCheckpoints(5_000L);
        env.setRestartStrategy(RestartStrategies.fixedDelayRestart(3, 10_000L));
        env.setStateBackend(new EmbeddedRocksDBStateBackend());

        Properties props = new Properties();
        props.setProperty("bootstrap.servers", System.getenv().getOrDefault("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092"));
        props.setProperty("group.id", System.getenv().getOrDefault("FLINK_RISK_GROUP_ID", "fraud-engine-v2"));

        KafkaSource<String> source = KafkaSource.<String>builder()
            .setBootstrapServers(props.getProperty("bootstrap.servers"))
            .setTopics("banking.transactions", "banking.login_events", "banking.sessions", "api_requests")
            .setGroupId(props.getProperty("group.id"))
            .setStartingOffsets(OffsetsInitializer.earliest())
            .setValueOnlyDeserializer(new SimpleStringSchema())
            .build();

        // NEW — banking.accounts CDC, joined via broadcast state (see
        // AccountEnrichmentFunction) to resolve the real customer_id for
        // transactions (account_id -> customer_id), replacing the
        // "acct:<from_account>" stopgap identity. Already provisioned:
        // table.include.list and kafka-init both already cover
        // public.accounts / banking.accounts, so no infra changes needed.
        KafkaSource<String> accountsSource = KafkaSource.<String>builder()
            .setBootstrapServers(props.getProperty("bootstrap.servers"))
            .setTopics("banking.accounts")
            .setGroupId(props.getProperty("group.id") + "-accounts")
            .setStartingOffsets(OffsetsInitializer.earliest())
            .setValueOnlyDeserializer(new SimpleStringSchema())
            .build();

        DataStream<String> rawStream = env.fromSource(source, WatermarkStrategy.noWatermarks(), "kafka-risk-source");
        DataStream<String> rawAccountsStream =
            env.fromSource(accountsSource, WatermarkStrategy.noWatermarks(), "kafka-accounts-source");

        DataStream<RiskEvent> normalized = rawStream.map(RiskEventNormalizer::normalize)
            .filter(event -> event != null); // Expressed via a unique lambda
        // NOTE: the identityKey != null filter now happens AFTER account
        // enrichment below, not here. Filtering here would permanently drop
        // any transaction whose account_id -> customer_id mapping hasn't
        // been broadcast yet — the whole point of doing this join is to
        // give those events a chance to resolve first.

        DataStream<AccountUpdate> accountUpdates = rawAccountsStream.map(AccountEventNormalizer::normalize)
            .filter(update -> update != null); // Expressed via a unique lambda

        BroadcastStream<AccountUpdate> accountsBroadcast =
            accountUpdates.broadcast(AccountEnrichmentFunction.ACCOUNT_STATE_DESCRIPTOR);

        DataStream<RiskEvent> enriched = normalized.connect(accountsBroadcast)
            .process(new AccountEnrichmentFunction())
            .name("account-enrichment");

        // CHANGED: filter on identityKey (customer_id — now resolved for
        // transactions via the accounts join above wherever possible, or
        // "ip:<addr>" for api_requests, or the "acct:<from_account>"
        // fallback if the account mapping hasn't arrived yet) instead of
        // customerId directly — previously every api_requests event was
        // silently dropped here, so SUSPICIOUS_API_PATTERN could never fire.
        DataStream<RiskEvent> withIdentity = enriched.filter(event -> event.getIdentityKey() != null);

        // NEW — Async I/O practice implementation (see OpenSearchLookupFunction
        // for the full rationale). Placed here, after the identityKey filter
        // (no point spending an async call on an event about to be dropped)
        // and before watermarking/keying, since the lookup result doesn't
        // affect either. unorderedWait is used rather than orderedWait since
        // this side-lookup has no ordering requirement -- events can complete
        // out of order without affecting correctness, which gives Flink more
        // freedom to pipeline concurrent requests. 100 max in-flight requests
        // bounds this so a slow/unreachable OpenSearch can't cause unbounded
        // queueing; 5s timeout matches the HTTP client's own request timeout
        // inside the function.
        DataStream<RiskEvent> withHistoricalContext = AsyncDataStream.unorderedWait(
            withIdentity,
            new OpenSearchLookupFunction(),
            5000L,
            TimeUnit.MILLISECONDS,
            100
        ).name("opensearch-historical-lookup");

        DataStream<FraudAlert> alerts = withHistoricalContext
            .assignTimestampsAndWatermarks(
                WatermarkStrategy.<RiskEvent>forBoundedOutOfOrderness(Duration.ofSeconds(300))
                    .withTimestampAssigner((event, ignored) -> event.getEventTime())
                    .withIdleness(Duration.ofSeconds(60))
            )
            .keyBy(RiskEvent::getIdentityKey)
            .process(new RiskScoringProcessor())
            .name("risk-scoring-processor");

        // CHANGED: alert routing split into two real Kafka topics instead of
        // one topic carrying a severity field, matching the topics
        // kafka-init already provisions (fraud_alerts_v2, critical_alerts_v2)
        // and the original dual-routing design. Simple filter-based split —
        // each filter re-scans the same `alerts` stream once, which costs a
        // bit of duplicate CPU versus a true Flink side-output split, but
        // avoids the extra OutputTag/Context.output() machinery at this
        // volume. Revisit with side outputs only if profiling shows this
        // filter pair is actually a bottleneck.
        String fraudTopic = System.getenv().getOrDefault("KAFKA_FRAUD_ALERT_TOPIC", "fraud_alerts_v2");
        String criticalTopic = System.getenv().getOrDefault("KAFKA_CRITICAL_ALERT_TOPIC", "critical_alerts_v2");
        String schemaRegistryUrl = System.getenv().getOrDefault("SCHEMA_REGISTRY_URL", RiskScoringConfig.SCHEMA_REGISTRY_URL);
        String fraudSchemaSubject = System.getenv().getOrDefault("SCHEMA_REGISTRY_FRAUD_SUBJECT", RiskScoringConfig.SCHEMA_REGISTRY_FRAUD_SUBJECT);
        String criticalSchemaSubject = System.getenv().getOrDefault("SCHEMA_REGISTRY_CRITICAL_SUBJECT", RiskScoringConfig.SCHEMA_REGISTRY_CRITICAL_SUBJECT);

        DataStream<FraudAlert> fraudAlerts = alerts.filter(a -> "FRAUD".equals(a.getSeverity()));
        DataStream<FraudAlert> criticalAlerts = alerts.filter(a -> "CRITICAL".equals(a.getSeverity()));

        fraudAlerts.sinkTo(buildKafkaSink(props.getProperty("bootstrap.servers"), fraudTopic, schemaRegistryUrl, fraudSchemaSubject))
            .name("fraud-alerts-sink");
        criticalAlerts.sinkTo(buildKafkaSink(props.getProperty("bootstrap.servers"), criticalTopic, schemaRegistryUrl, criticalSchemaSubject))
            .name("critical-alerts-sink");

        env.execute("banking-fraud-risk-engine-v2");
    }

    private static KafkaSink<FraudAlert> buildKafkaSink(String bootstrapServers, String topic,
                                                         String schemaRegistryUrl, String schemaSubject) {
        return KafkaSink.<FraudAlert>builder()
            .setBootstrapServers(bootstrapServers)
            .setRecordSerializer(KafkaRecordSerializationSchema.<FraudAlert>builder()
                .setTopic(topic)
                .setValueSerializationSchema(new FraudAlertAvroSerializationSchema(schemaRegistryUrl, schemaSubject))
                .build()
            )
            .setDeliverGuarantee(DeliveryGuarantee.AT_LEAST_ONCE)
            .build();
    }
}