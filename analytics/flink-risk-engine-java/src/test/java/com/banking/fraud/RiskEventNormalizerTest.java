package com.banking.fraud;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;

public class RiskEventNormalizerTest {
    @Test
    public void shouldUnwrapDebeziumEnvelopeAndExtractIndicator() {
        String payload = "{\"op\":\"u\",\"ts_ms\":1700000000000,\"after\":{\"customer_id\":\"cust-123\",\"account_id\":\"acct-001\",\"risk_indicator_type\":\"NEW_DEVICE\"},\"source\":{\"lsn\":\"1234\"}}";

        RiskEvent event = RiskEventNormalizer.normalize(payload);

        assertNotNull(event);
        assertEquals("cust-123", event.getCustomerId());
        assertEquals("NEW_DEVICE", event.getIndicator());
        assertEquals("u", event.getSourceOperation());
    }

    @Test
    public void shouldIgnoreDeleteOperations() {
        String payload = "{\"op\":\"d\",\"after\":null,\"source\":{\"table\":\"public.transactions\"}}";
        assertNull(RiskEventNormalizer.normalize(payload));
    }

    @Test
    public void shouldDeriveDedupKeyFromSourceTableWhenLsnIsMissing() {
        String payload = "{\"op\":\"u\",\"after\":{\"customer_id\":\"cust-456\",\"event_id\":\"evt-001\"},\"source\":{\"table\":\"public.transactions\"}}";

        RiskEvent event = RiskEventNormalizer.normalize(payload);

        assertNotNull(event);
        assertEquals("transactions:evt-001", event.getDedupKey());
    }

    // --- NEW: raw-field extraction for indicator derivation ---

    @Test
    public void shouldExtractRawFieldsFromLoginEventsForIndicatorDerivation() {
        String payload = "{\"op\":\"c\",\"ts_ms\":1700000000000,"
            + "\"after\":{\"customer_id\":\"cust-789\",\"device_id\":\"dev-1\",\"country\":\"KE\",\"success\":false},"
            + "\"source\":{\"table\":\"public.login_events\",\"lsn\":\"9999\"}}";

        RiskEvent event = RiskEventNormalizer.normalize(payload);

        assertNotNull(event);
        assertEquals("login_events", event.getSourceTable());
        assertEquals("dev-1", event.getDeviceId());
        assertEquals("KE", event.getCountry());
        assertEquals(Boolean.FALSE, event.getLoginSuccess());
        assertNull(event.getIndicator(), "no explicit indicator field present, so derivation should be left to the processor");
    }

    @Test
    public void shouldExtractAmountAndBeneficiaryFromTransactions() {
        String payload = "{\"op\":\"c\",\"ts_ms\":1700000000000,"
            + "\"after\":{\"customer_id\":\"cust-321\",\"amount\":15000.50,\"beneficiary_id\":\"ben-1\"},"
            + "\"source\":{\"table\":\"public.transactions\",\"lsn\":\"1111\"}}";

        RiskEvent event = RiskEventNormalizer.normalize(payload);

        assertNotNull(event);
        assertEquals("transactions", event.getSourceTable());
        assertEquals(15000.50, event.getAmount());
        assertEquals("ben-1", event.getBeneficiaryId());
    }

    @Test
    public void shouldFallBackToIpAddressIdentityForApiRequestsWithNoCustomerId() {
        // api_request.avsc has no customer_id field at all — this is the
        // fallback that keeps api_requests events from being silently
        // dropped by the identityKey != null filter in RiskScoringJob.
        String payload = "{\"request_id\":\"req-1\",\"ip_address\":\"10.0.0.5\",\"endpoint\":\"/login\","
            + "\"method\":\"POST\",\"status_code\":401,\"timestamp\":1700000000000}";

        RiskEvent event = RiskEventNormalizer.normalize(payload);

        assertNotNull(event);
        assertEquals("api_requests", event.getSourceTable());
        assertNull(event.getCustomerId());
        assertEquals("ip:10.0.0.5", event.getIdentityKey());
    }
}
