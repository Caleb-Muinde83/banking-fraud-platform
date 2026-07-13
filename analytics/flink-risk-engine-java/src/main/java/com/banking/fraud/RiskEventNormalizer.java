package com.banking.fraud;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

/**
 * Normalizes both Debezium CDC envelopes ({before, after, source, op, ts_ms})
 * and raw edge telemetry (api_requests) into a common RiskEvent shape.
 *
 * Field-name assumptions below have been checked against the real
 * SQLAlchemy models (models.py):
 *  - login_events: customer_id, device_id, country, ip_address, success  ✓ confirmed correct
 *  - transactions: from_account, to_account, amount, currency,
 *    transaction_type, timestamp — NO customer_id, NO beneficiary_id.
 *    (earlier assumption of a beneficiary_id/customer_id column was wrong;
 *    corrected here to read from_account/to_account instead)
 *  - sessions: session_id, customer_id, device_id, ip_address, created_at,
 *    expires_at — NO country/geo field. IMPOSSIBLE_TRAVEL can't be derived
 *    from sessions with this schema; it's derived from login_events
 *    country-change timing instead (see RiskScoringProcessor).
 */
public class RiskEventNormalizer {
    private static final ObjectMapper MAPPER = new ObjectMapper();

    public static RiskEvent normalize(String rawValue) {
        try {
            JsonNode root = MAPPER.readTree(rawValue);
            boolean isCdcEnvelope = root.has("source") || root.has("op");
            String operation = root.path("op").asText("u");
            if (isCdcEnvelope && "d".equalsIgnoreCase(operation)) {
                return null;
            }

            JsonNode payload = root.has("after") && !root.get("after").isNull() ? root.get("after") : root;

            String rawTable = root.path("source").path("table").asText(null);
            String sourceTable = stripSchemaPrefix(rawTable);
            if (sourceTable == null && !isCdcEnvelope && payload.has("endpoint")) {
                // Edge telemetry (api_requests) carries no CDC envelope/source block.
                sourceTable = "api_requests";
            }

            String customerId = firstNonBlank(
                payload.path("customer_id").asText(null),
                root.path("customer_id").asText(null)
            );
            String accountId = firstNonBlank(
                payload.path("account_id").asText(null),
                root.path("account_id").asText(null)
            );
            // CONFIRMED against the real Transaction model: it has
            // from_account/to_account, not customer_id or beneficiary_id.
            // Transactions therefore cannot be tied to a customer directly
            // from this topic alone — a proper fix joins banking.accounts
            // (account_id -> customer_id) via broadcast/lookup state. Until
            // that's added, we fall back to keying transactions by the
            // paying account instead of silently dropping them.
            String fromAccountId = payload.path("from_account").asText(null);
            String toAccountId = payload.path("to_account").asText(null);
            String ipAddress = firstNonBlank(
                payload.path("ip_address").asText(null),
                root.path("ip_address").asText(null)
            );
            String identityKey;
            if (customerId != null) {
                identityKey = customerId;
            } else if (ipAddress != null) {
                // api_requests has no customer_id (see api_request.avsc).
                identityKey = "ip:" + ipAddress;
            } else if (fromAccountId != null) {
                // transactions has no customer_id — interim account-level
                // identity until the accounts-topic join exists.
                identityKey = "acct:" + fromAccountId;
            } else {
                identityKey = null;
            }

            // Explicit override: honored if an upstream source already labels
            // its own indicator (kept for back-compat / tests). Otherwise the
            // processor derives indicators itself from the raw fields below.
            String explicitIndicator = firstNonBlank(
                payload.path("risk_indicator_type").asText(null),
                payload.path("indicator").asText(null),
                root.path("risk_indicator_type").asText(null)
            );

            String country = firstNonBlank(
                payload.path("country").asText(null),
                payload.path("login_country").asText(null)
            );
            String deviceId = payload.path("device_id").asText(null);

            JsonNode successNode = payload.path("success");
            Boolean loginSuccess = successNode.isMissingNode() || successNode.isNull()
                ? null : successNode.asBoolean();

            JsonNode amountNode = payload.path("amount");
            Double amount = amountNode.isMissingNode() || amountNode.isNull()
                ? null : amountNode.asDouble();

            // NOTE: beneficiary_id does not exist on Transaction rows — the
            // recipient is `to_account`, already extracted above as
            // toAccountId. Kept as a fallback for any source that does
            // supply an explicit beneficiary_id (e.g. a future enrichment).
            String beneficiaryId = firstNonBlank(payload.path("beneficiary_id").asText(null), toAccountId);

            String eventId = firstNonBlank(
                root.path("event_id").asText(null),
                payload.path("event_id").asText(null),
                payload.path("request_id").asText(null),
                root.path("source").path("lsn").asText(null)
            );

            long eventTime = root.path("ts_ms").asLong(
                payload.path("event_time").asLong(
                    payload.path("timestamp").asLong(System.currentTimeMillis())
                )
            );

            String dedupKey = firstNonBlank(
                root.path("source").path("lsn").asText(null),
                root.path("source").path("txId").asText(null),
                payload.path("source_event_id").asText(null),
                sourceTable != null && eventId != null ? sourceTable + ":" + eventId : null,
                eventId
            );

            RiskEvent event = new RiskEvent(
                eventId, customerId, accountId, explicitIndicator, eventTime,
                "unknown", operation, dedupKey, null
            );
            event.setIdentityKey(identityKey);
            event.setSourceTable(sourceTable);
            event.setCountry(country);
            event.setDeviceId(deviceId);
            event.setLoginSuccess(loginSuccess);
            event.setAmount(amount);
            event.setBeneficiaryId(beneficiaryId);
            event.setIpAddress(ipAddress);
            event.setFromAccountId(fromAccountId);
            event.setToAccountId(toAccountId);
            if (accountId != null) {
                event.setAccountId(accountId);
            } else if (fromAccountId != null) {
                event.setAccountId(fromAccountId);
            }
            return event;
        } catch (Exception ex) {
            return null;
        }
    }

    private static String stripSchemaPrefix(String rawTable) {
        if (rawTable == null) {
            return null;
        }
        int dot = rawTable.lastIndexOf('.');
        return dot >= 0 ? rawTable.substring(dot + 1) : rawTable;
    }

    private static String firstNonBlank(String... values) {
        for (String value : values) {
            if (value != null && !value.isBlank()) {
                return value;
            }
        }
        return null;
    }
}