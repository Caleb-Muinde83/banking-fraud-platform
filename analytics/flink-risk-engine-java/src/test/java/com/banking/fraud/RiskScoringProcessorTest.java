package com.banking.fraud;

import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class RiskScoringProcessorTest {

    // NOTE: the previous version of this test was a no-op (`assertTrue(true)`)
    // and did not exercise any behavior. Replaced with real coverage of
    // deriveIndicators, which is package-private specifically so it can be
    // tested without a full Flink KeyedProcessFunction test harness.

    @Test
    public void shouldUseConfiguredWeights() {
        Integer weight = RiskScoringConfig.DEFAULT_WEIGHTS.get("NEW_DEVICE");
        assertEquals(30, weight);
    }

    @Test
    public void shouldKeepIndicatorListBounded() {
        RiskProfileState state = new RiskProfileState();
        for (int i = 0; i < 12; i++) {
            state.addIndicator("IND" + i);
        }
        assertEquals(8, state.getActiveIndicators().size());
    }

    @Test
    public void shouldBuildDeterministicAlertIdsFromCooldownWindow() {
        String idA = RiskScoringProcessor.buildDeterministicAlertId("cust-123", "FRAUD", 1_700_000_000_000L);
        String idB = RiskScoringProcessor.buildDeterministicAlertId("cust-123", "FRAUD", 1_700_000_000_500L);
        assertEquals(idA, idB);
    }

    @Test
    public void shouldDetectNewCountryForLoginEvents() {
        RiskScoringProcessor processor = new RiskScoringProcessor();
        RiskProfileState state = new RiskProfileState();
        state.setLastKnownCountry("KE");

        RiskEvent event = new RiskEvent();
        event.setSourceTable("login_events");
        event.setCountry("NG");

        List<String> indicators = processor.deriveIndicators(event, state, 1_000L);
        assertTrue(indicators.contains("NEW_COUNTRY"));
    }

    @Test
    public void shouldNotFlagNewCountryOnFirstEverLogin() {
        // No prior country on record -> nothing to compare against, no false positive.
        RiskScoringProcessor processor = new RiskScoringProcessor();
        RiskProfileState state = new RiskProfileState();

        RiskEvent event = new RiskEvent();
        event.setSourceTable("login_events");
        event.setCountry("KE");

        List<String> indicators = processor.deriveIndicators(event, state, 1_000L);
        assertFalse(indicators.contains("NEW_COUNTRY"));
        assertEquals("KE", state.getLastKnownCountry());
    }

    @Test
    public void shouldDetectVelocityViolationAfterRepeatedTransactions() {
        RiskScoringProcessor processor = new RiskScoringProcessor();
        RiskProfileState state = new RiskProfileState();

        RiskEvent event = new RiskEvent();
        event.setSourceTable("transactions");
        event.setAmount(50.0);

        List<String> lastResult = null;
        for (int i = 0; i < RiskScoringConfig.VELOCITY_COUNT_THRESHOLD; i++) {
            lastResult = processor.deriveIndicators(event, state, i * 1_000L);
        }
        assertTrue(lastResult.contains("VELOCITY_VIOLATION"));
    }

    @Test
    public void shouldDetectLargeTransferAmount() {
        RiskScoringProcessor processor = new RiskScoringProcessor();
        RiskProfileState state = new RiskProfileState();

        RiskEvent event = new RiskEvent();
        event.setSourceTable("transactions");
        event.setAmount(RiskScoringConfig.LARGE_TRANSFER_THRESHOLD + 1);

        List<String> indicators = processor.deriveIndicators(event, state, 1_000L);
        assertTrue(indicators.contains("LARGE_TRANSFER_AMOUNT"));
    }

    @Test
    public void shouldDetectSuspiciousApiPatternAfterRateThresholdExceeded() {
        RiskScoringProcessor processor = new RiskScoringProcessor();
        RiskProfileState state = new RiskProfileState();

        RiskEvent event = new RiskEvent();
        event.setSourceTable("api_requests");

        List<String> lastResult = null;
        for (int i = 0; i < RiskScoringConfig.API_RATE_THRESHOLD; i++) {
            lastResult = processor.deriveIndicators(event, state, i * 100L);
        }
        assertTrue(lastResult.contains("SUSPICIOUS_API_PATTERN"));
    }

    @Test
    public void explicitIndicatorOverrideShortCircuitsDerivation() {
        // Back-compat path: if an event already carries a labeled indicator,
        // honor it directly rather than deriving from raw fields.
        RiskScoringProcessor processor = new RiskScoringProcessor();
        RiskProfileState state = new RiskProfileState();

        RiskEvent event = new RiskEvent();
        event.setSourceTable("transactions");
        event.setIndicator("HIGH_RISK_RECIPIENT");
        event.setAmount(1.0); // would not otherwise trigger anything

        List<String> indicators = processor.deriveIndicators(event, state, 1_000L);
        assertEquals(List.of("HIGH_RISK_RECIPIENT"), indicators);
    }
}
