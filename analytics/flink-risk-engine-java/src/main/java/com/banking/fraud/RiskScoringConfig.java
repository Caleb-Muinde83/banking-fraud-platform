package com.banking.fraud;

import java.io.IOException;
import java.io.InputStream;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

public class RiskScoringConfig {
    private static final Properties PROPERTIES = loadProperties();

    public static final int ALERT_THRESHOLD = Integer.parseInt(PROPERTIES.getProperty("alert.threshold", "80"));
    public static final int CRITICAL_THRESHOLD = Integer.parseInt(PROPERTIES.getProperty("alert.critical-threshold", "100"));
    public static final int COOLDOWN_MS = Integer.parseInt(PROPERTIES.getProperty("alert.cooldown-ms", String.valueOf(15 * 60 * 1000)));
    public static final int TTL_MS = Integer.parseInt(PROPERTIES.getProperty("state.ttl-ms", String.valueOf(24 * 60 * 60 * 1000)));
    public static final int DECAY_MS = Integer.parseInt(PROPERTIES.getProperty("state.decay-ms", String.valueOf(30 * 60 * 1000)));

    // NEW — indicator derivation thresholds. Previously the engine only
    // scored a pre-supplied indicator; nothing computed these from raw data.
    public static final double LARGE_TRANSFER_THRESHOLD =
        Double.parseDouble(PROPERTIES.getProperty("indicator.large-transfer-threshold", "10000"));
    public static final long VELOCITY_WINDOW_MS =
        Long.parseLong(PROPERTIES.getProperty("indicator.velocity-window-ms", String.valueOf(5 * 60 * 1000)));
    public static final int VELOCITY_COUNT_THRESHOLD =
        Integer.parseInt(PROPERTIES.getProperty("indicator.velocity-count-threshold", "4"));
    public static final long FAILED_LOGIN_WINDOW_MS =
        Long.parseLong(PROPERTIES.getProperty("indicator.failed-login-window-ms", String.valueOf(5 * 60 * 1000)));
    public static final int FAILED_LOGIN_THRESHOLD =
        Integer.parseInt(PROPERTIES.getProperty("indicator.failed-login-threshold", "3"));
    public static final long IMPOSSIBLE_TRAVEL_WINDOW_MS =
        Long.parseLong(PROPERTIES.getProperty("indicator.impossible-travel-window-ms", String.valueOf(2L * 60 * 60 * 1000)));
    public static final long API_RATE_WINDOW_MS =
        Long.parseLong(PROPERTIES.getProperty("indicator.api-rate-window-ms", String.valueOf(60 * 1000)));
    public static final int API_RATE_THRESHOLD =
        Integer.parseInt(PROPERTIES.getProperty("indicator.api-rate-threshold", "50"));
    public static final Set<String> HIGH_RISK_RECIPIENTS = loadHighRiskRecipients();

    // NEW — Schema Registry wiring. Previously Avro was serialized with a
    // local, unregistered schema (no registry round-trip at all).
    public static final String SCHEMA_REGISTRY_URL =
        PROPERTIES.getProperty("schema-registry.url", "http://schema-registry:8081");
    // CHANGED: was a single SCHEMA_REGISTRY_SUBJECT — split into two subjects
    // now that alerts route to two real topics (fraud_alerts_v2,
    // critical_alerts_v2) instead of one topic with a severity field.
    public static final String SCHEMA_REGISTRY_FRAUD_SUBJECT =
        PROPERTIES.getProperty("schema-registry.fraud-subject", "fraud_alerts_v2-value");
    public static final String SCHEMA_REGISTRY_CRITICAL_SUBJECT =
        PROPERTIES.getProperty("schema-registry.critical-subject", "critical_alerts_v2-value");

    public static final Map<String, Integer> DEFAULT_WEIGHTS = loadWeights();

    private static Properties loadProperties() {
        Properties properties = new Properties();
        try (InputStream input = RiskScoringConfig.class.getClassLoader().getResourceAsStream("risk-scoring.properties")) {
            if (input != null) {
                properties.load(input);
            }
        } catch (IOException e) {
            throw new IllegalStateException("Unable to load risk scoring properties", e);
        }
        return properties;
    }

    private static Map<String, Integer> loadWeights() {
        Map<String, Integer> weights = new HashMap<>();
        for (String key : PROPERTIES.stringPropertyNames()) {
            if (key.startsWith("weights.")) {
                weights.put(key.substring("weights.".length()), Integer.parseInt(PROPERTIES.getProperty(key)));
            }
        }
        return Collections.unmodifiableMap(weights);
    }

    private static Set<String> loadHighRiskRecipients() {
        String raw = PROPERTIES.getProperty("indicator.high-risk-recipients", "");
        if (raw == null || raw.isBlank()) {
            return Collections.emptySet();
        }
        Set<String> recipients = new HashSet<>(Arrays.asList(raw.split(",")));
        recipients.removeIf(String::isBlank);
        return Collections.unmodifiableSet(recipients);
    }
}