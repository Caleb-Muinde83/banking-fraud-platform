package com.banking.fraud;

import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.StateTtlConfig;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

public class RiskScoringProcessor extends KeyedProcessFunction<String, RiskEvent, FraudAlert> {
    private transient ValueState<RiskProfileState> profileState;
    
    // UPGRADED: Using MapState instead of ValueState<Map> for O(1) RocksDB serialization
    private transient MapState<String, Long> emittedAlertIds;
    private transient MapState<String, Long> seenEventKeys;

    @Override
    public void open(Configuration parameters) throws Exception {
        StateTtlConfig ttlConfig = StateTtlConfig.newBuilder(Time.milliseconds(RiskScoringConfig.TTL_MS))
            .setUpdateType(StateTtlConfig.UpdateType.OnCreateAndWrite)
            .setStateVisibility(StateTtlConfig.StateVisibility.NeverReturnExpired)
            // Actual Flink 1.18 API for background reclamation on the
            // RocksDB backend (EmbeddedRocksDBStateBackend, per
            // RiskScoringJob) — cleanupInBackground() does not exist on
            // StateTtlConfig.Builder; that was my mistake in the previous
            // patch and broke the build. This runs the cleanup filter during
            // RocksDB compaction, checking wall-clock time after every 1000
            // processed state entries so it doesn't slow down compaction.
            .cleanupInRocksdbCompactFilter(1000)
            .build();

        ValueStateDescriptor<RiskProfileState> profileDescriptor =
            new ValueStateDescriptor<>("profile-state", TypeInformation.of(RiskProfileState.class));
        profileDescriptor.enableTimeToLive(ttlConfig);
        profileState = getRuntimeContext().getState(profileDescriptor);

        MapStateDescriptor<String, Long> alertIdsDescriptor =
            new MapStateDescriptor<>("emitted-alert-ids", String.class, Long.class);
        alertIdsDescriptor.enableTimeToLive(ttlConfig);
        emittedAlertIds = getRuntimeContext().getMapState(alertIdsDescriptor);

        MapStateDescriptor<String, Long> dedupDescriptor =
            new MapStateDescriptor<>("seen-event-keys", String.class, Long.class);
        dedupDescriptor.enableTimeToLive(ttlConfig);
        seenEventKeys = getRuntimeContext().getMapState(dedupDescriptor);
    }

    @Override
    public void processElement(RiskEvent event, Context ctx, Collector<FraudAlert> out) throws Exception {
        if (event == null || event.getIdentityKey() == null) {
            return;
        }

        RiskProfileState state = profileState.value();
        if (state == null) {
            state = new RiskProfileState();
        }

        long now = event.getEventTime();

        // --- Dedup (bounded, pruned by TTL_MS rather than growing forever) ---
        pruneOlderThan(seenEventKeys, now, RiskScoringConfig.TTL_MS);
        if (event.getDedupKey() != null) {
            if (seenEventKeys.contains(event.getDedupKey())) {
                profileState.update(state);
                return;
            }
            seenEventKeys.put(event.getDedupKey(), now);
        }

        // --- Score decay ---
        applyDecay(state, now);

        // --- Indicator derivation ---
        List<String> indicators = deriveIndicators(event, state, now);

        if (!indicators.isEmpty()) {
            int weightSum = 0;
            for (String indicator : indicators) {
                weightSum += RiskScoringConfig.DEFAULT_WEIGHTS.getOrDefault(indicator, 10);
                state.addIndicator(indicator);
            }
            state.setCumulativeScore(state.getCumulativeScore() + weightSum);
            state.setLastScoreUpdateTs(now);
        }

        // Ensure a decay timer is pending whenever there's a live score
        if (state.getCumulativeScore() > 0 && state.getNextDecayTimerTs() <= now) {
            long nextTs = now + RiskScoringConfig.DECAY_MS;
            ctx.timerService().registerEventTimeTimer(nextTs);
            state.setNextDecayTimerTs(nextTs);
        }

        boolean shouldAlert = state.getCumulativeScore() > RiskScoringConfig.ALERT_THRESHOLD;
        boolean shouldEscalate = state.getCumulativeScore() >= RiskScoringConfig.CRITICAL_THRESHOLD;

        if (shouldAlert) {
            long alertTs = state.getLastAlertTs();
            boolean cooldownExpired = alertTs == 0 || (now - alertTs) >= RiskScoringConfig.COOLDOWN_MS;
            if (cooldownExpired) {
                String severity = shouldEscalate ? "CRITICAL" : "FRAUD";
                String alertId = buildDeterministicAlertId(event.getIdentityKey(), severity, now);

                pruneOlderThan(emittedAlertIds, now, RiskScoringConfig.TTL_MS);

                if (!emittedAlertIds.contains(alertId)) {
                    FraudAlert alert = new FraudAlert(
                        alertId,
                        event.getCustomerId(),
                        event.getAccountId(),
                        now,
                        severity,
                        state.getCumulativeScore(),
                        RiskScoringConfig.ALERT_THRESHOLD,
                        new ArrayList<>(state.getActiveIndicators()),
                        indicators.isEmpty() ? event.getIndicator() : indicators.get(indicators.size() - 1),
                        now,
                        now,
                        new ArrayList<>(List.of(event.getEventId() == null ? "unknown" : event.getEventId())), //  Wrapped in ArrayList
                        shouldEscalate ? "LOCK_ACCOUNT" : "REQUIRE_MFA"
                    );
                    state.setLastAlertTs(now);
                    emittedAlertIds.put(alertId, now);
                    out.collect(alert);
                }
            }
        }

        profileState.update(state);
    }

    @Override
    public void onTimer(long timestamp, OnTimerContext ctx, Collector<FraudAlert> out) throws Exception {
        RiskProfileState state = profileState.value();
        if (state == null) {
            return;
        }
        applyDecay(state, timestamp);
        if (state.getCumulativeScore() > 0) {
            long nextTs = timestamp + RiskScoringConfig.DECAY_MS;
            ctx.timerService().registerEventTimeTimer(nextTs);
            state.setNextDecayTimerTs(nextTs);
        } else {
            state.setNextDecayTimerTs(0);
        }
        profileState.update(state);
    }

    /** Halves the cumulative score for each full DECAY_MS interval elapsed since the last decay. */
    private void applyDecay(RiskProfileState state, long now) {
        long last = state.getLastDecayAppliedTs() > 0 ? state.getLastDecayAppliedTs() : state.getLastScoreUpdateTs();
        if (last <= 0) {
            state.setLastDecayAppliedTs(now);
            return;
        }
        long elapsed = now - last;
        if (elapsed <= 0 || RiskScoringConfig.DECAY_MS <= 0) {
            return;
        }
        int steps = (int) Math.min(elapsed / RiskScoringConfig.DECAY_MS, 10);
        if (steps <= 0) {
            return;
        }
        int decayed = state.getCumulativeScore();
        for (int i = 0; i < steps; i++) {
            decayed = decayed / 2;
        }
        state.setCumulativeScore(decayed);
        state.setLastDecayAppliedTs(last + (long) steps * RiskScoringConfig.DECAY_MS);
    }

    /**
     * Derives fraud indicators from raw event fields compared against stored profile state.
     */
    List<String> deriveIndicators(RiskEvent event, RiskProfileState state, long now) {
        List<String> indicators = new ArrayList<>();

        if (event.getIndicator() != null) {
            indicators.add(event.getIndicator());
            return indicators;
        }

        String table = event.getSourceTable() == null ? "" : event.getSourceTable();

        switch (table) {
            case "login_events":
                if (event.getDeviceId() != null) {
                    if (state.getLastKnownDeviceId() != null && !state.getLastKnownDeviceId().equals(event.getDeviceId())) {
                        indicators.add("NEW_DEVICE");
                    }
                    state.setLastKnownDeviceId(event.getDeviceId());
                }
                if (event.getCountry() != null) {
                    boolean countryChanged = state.getLastKnownCountry() != null
                        && !state.getLastKnownCountry().equals(event.getCountry());
                    if (countryChanged) {
                        indicators.add("NEW_COUNTRY");
                        // CHANGED: sessions carries no country/geo column in
                        // the real schema, so IMPOSSIBLE_TRAVEL is derived
                        // here instead — a country change that happens
                        // faster than physically plausible travel, timed
                        // against the previous login_events row for this
                        // identity.
                        if (state.getLastLoginEventTs() > 0
                            && (now - state.getLastLoginEventTs()) <= RiskScoringConfig.IMPOSSIBLE_TRAVEL_WINDOW_MS) {
                            indicators.add("IMPOSSIBLE_TRAVEL");
                        }
                    }
                    state.setLastKnownCountry(event.getCountry());
                }
                state.setLastLoginEventTs(now);
                if (Boolean.FALSE.equals(event.getLoginSuccess())) {
                    int failedCount = state.recordFailedLogin(now, RiskScoringConfig.FAILED_LOGIN_WINDOW_MS);
                    if (failedCount >= RiskScoringConfig.FAILED_LOGIN_THRESHOLD) {
                        indicators.add("MULTIPLE_FAILED_LOGINS");
                    }
                } else if (Boolean.TRUE.equals(event.getLoginSuccess())) {
                    state.clearFailedLogins();
                }
                break;

            case "transactions":
                if (event.getAmount() != null && event.getAmount() > RiskScoringConfig.LARGE_TRANSFER_THRESHOLD) {
                    indicators.add("LARGE_TRANSFER_AMOUNT");
                }
                int txCount = state.recordTransaction(now, RiskScoringConfig.VELOCITY_WINDOW_MS);
                if (txCount >= RiskScoringConfig.VELOCITY_COUNT_THRESHOLD) {
                    indicators.add("VELOCITY_VIOLATION");
                }
                // CHANGED: Transaction has no beneficiary_id column — the
                // recipient is to_account. HIGH_RISK_RECIPIENTS now holds
                // high-risk account numbers, not beneficiary IDs.
                if (event.getToAccountId() != null
                    && RiskScoringConfig.HIGH_RISK_RECIPIENTS.contains(event.getToAccountId())) {
                    indicators.add("HIGH_RISK_RECIPIENT");
                }
                break;

            case "api_requests":
                int apiCount = state.recordApiRequest(now, RiskScoringConfig.API_RATE_WINDOW_MS);
                if (apiCount >= RiskScoringConfig.API_RATE_THRESHOLD) {
                    indicators.add("SUSPICIOUS_API_PATTERN");
                }
                break;

            // "sessions" intentionally has no case: the real Session model
            // (session_id, customer_id, device_id, ip_address, created_at,
            // expires_at) carries none of the fields the original taxonomy
            // needed (no country/geo). Sessions events still flow through
            // dedup/state but contribute no indicator until a genuine
            // geo-capable signal is available on that table.
            default:
                break;
        }

        return indicators;
    }

    /** Overloaded prune method to iterate natively over MapState entries */
    private static void pruneOlderThan(MapState<String, Long> mapState, long now, long maxAgeMs) throws Exception {
        Iterator<Map.Entry<String, Long>> iterator = mapState.iterator();
        while (iterator.hasNext()) {
            Map.Entry<String, Long> entry = iterator.next();
            if (now - entry.getValue() > maxAgeMs) {
                iterator.remove();
            }
        }
    }

    static String buildDeterministicAlertId(String identityKey, String severity, long eventTime) {
        long window = Math.max(1L, RiskScoringConfig.COOLDOWN_MS);
        long windowStart = (eventTime / window) * window;
        String safeIdentity = identityKey.replaceAll("[^a-zA-Z0-9]", "");
        return String.format("%s-%s-%d", safeIdentity, severity.toLowerCase(), windowStart);
    }
}