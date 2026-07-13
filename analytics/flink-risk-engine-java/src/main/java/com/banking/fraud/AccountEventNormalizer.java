package com.banking.fraud;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

/**
 * Normalizes banking.accounts CDC events (accounts table: account_id,
 * customer_id, balance, currency, status, created_at — confirmed against
 * schema.sql) into account_id -> customer_id updates for the broadcast join
 * in AccountEnrichmentFunction.
 */
public class AccountEventNormalizer {
    private static final ObjectMapper MAPPER = new ObjectMapper();

    public static AccountUpdate normalize(String rawValue) {
        try {
            JsonNode root = MAPPER.readTree(rawValue);
            String operation = root.path("op").asText("u");

            if ("d".equalsIgnoreCase(operation)) {
                // On delete, `after` is null — the account_id being removed
                // is in `before`.
                JsonNode before = root.path("before");
                String accountId = before.path("account_id").asText(null);
                if (accountId == null) {
                    return null;
                }
                return new AccountUpdate(accountId, null, true);
            }

            JsonNode after = root.has("after") && !root.get("after").isNull() ? root.get("after") : root;
            String accountId = after.path("account_id").asText(null);
            String customerId = after.path("customer_id").asText(null);
            if (accountId == null || customerId == null) {
                return null;
            }
            return new AccountUpdate(accountId, customerId, false);
        } catch (Exception ex) {
            return null;
        }
    }
}