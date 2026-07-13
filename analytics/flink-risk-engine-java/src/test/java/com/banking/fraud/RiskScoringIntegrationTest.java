package com.banking.fraud;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.CloseableIterator;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class RiskScoringIntegrationTest {

    // FIXED: the original version of this test built all four events with
    // sourceTable="transactions" and expected a country change to trigger
    // NEW_COUNTRY. That can't happen against the real schema — the
    // Transaction model (from_account, to_account, amount, currency,
    // transaction_type, timestamp) has no country column at all, and
    // NEW_COUNTRY is only ever derived from login_events. That mismatch is
    // why this failed (expected 1 alert, got 0): the intended score sequence
    // (35 + 35 + 30 = 100) silently lost the first +35, so cumulative score
    // only reached 65 and never crossed the 80 threshold.
    //
    // Rewritten below to stay entirely on realistic transactions-table
    // fields (amount only) so the "single jump from <80 straight to 100 on
    // the last event" property still holds, without relying on a field the
    // real schema doesn't have.
    @Test
    public void shouldTriggerCriticalAlertOnVelocityAttack() throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironment();
        env.setParallelism(1);

        long baseTime = 1700000000000L;
        String targetCustomer = "cust_integration_99";

        // txCount after each event: 1, 2, 3, 4 -> VELOCITY_VIOLATION fires
        // exactly on e4 (VELOCITY_COUNT_THRESHOLD default = 4).
        RiskEvent e1 = createMockTransaction(targetCustomer, 100.0, baseTime);          // below LARGE_TRANSFER threshold, no indicator -> score 0
        RiskEvent e2 = createMockTransaction(targetCustomer, 15000.0, baseTime + 2000); // LARGE_TRANSFER (+35) -> score 35
        RiskEvent e3 = createMockTransaction(targetCustomer, 15000.0, baseTime + 4000); // LARGE_TRANSFER (+35) -> score 70
        RiskEvent e4 = createMockTransaction(targetCustomer, 500.0, baseTime + 6000);   // VELOCITY_VIOLATION (+30) -> score 100 -> CRITICAL

        CloseableIterator<FraudAlert> results = env.fromElements(e1, e2, e3, e4)
            .assignTimestampsAndWatermarks(
                WatermarkStrategy.<RiskEvent>forBoundedOutOfOrderness(Duration.ofSeconds(1))
                    .withTimestampAssigner((event, timestamp) -> event.getEventTime())
            )
            .keyBy(RiskEvent::getIdentityKey)
            .process(new RiskScoringProcessor())
            .executeAndCollect();

        List<FraudAlert> alerts = new ArrayList<>();
        results.forEachRemaining(alerts::add);

        // Score stays below 80 through e1-e3 (0, 35, 70) and only crosses
        // both the FRAUD(80) and CRITICAL(100) thresholds in the same step
        // at e4 — so there's exactly one crossing event and exactly one alert.
        assertEquals(1, alerts.size(), "Should emit exactly one alert (single threshold crossing at e4)");

        FraudAlert finalAlert = alerts.get(0);
        assertEquals("CRITICAL", finalAlert.getSeverity());
        assertTrue(finalAlert.getCumulativeScore() >= 100, "Score should be at least 100");
        assertEquals("LOCK_ACCOUNT", finalAlert.getActionRequired());
    }

    private RiskEvent createMockTransaction(String customerId, double amount, long time) {
        RiskEvent event = new RiskEvent();
        // NOTE: real banking.transactions CDC events carry from_account/
        // to_account, not customer_id — this test operates at the
        // RiskScoringProcessor level (bypassing RiskEventNormalizer) and
        // sets identityKey directly, so it's exercising the scoring logic
        // in isolation from the normalizer's account->customer resolution
        // gap. See RiskEventNormalizer for how a real transaction event
        // resolves identity in production (falls back to "acct:<from_account>").
        event.setIdentityKey(customerId);
        event.setCustomerId(customerId);
        event.setSourceTable("transactions");
        event.setAmount(amount);
        event.setEventTime(time);
        event.setDedupKey(customerId + "-" + time);
        return event;
    }
}