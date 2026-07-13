package com.banking.fraud;

import org.apache.flink.api.common.state.BroadcastState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.streaming.api.functions.co.BroadcastProcessFunction;
import org.apache.flink.util.Collector;

/**
 * Resolves the real customer_id for transaction events by joining against
 * the banking.accounts stream (account_id -> customer_id), replacing the
 * "acct:<from_account>" stopgap identity set in RiskEventNormalizer
 * wherever the mapping is available.
 *
 * Uses Flink's broadcast state pattern: banking.accounts is small and
 * slowly-changing relative to transaction volume, so it's broadcast in
 * full to every parallel instance of this function rather than
 * partitioned/keyed. The main RiskEvent stream flows through un-keyed at
 * this stage — repartitioning by the (now possibly resolved) identityKey
 * happens downstream in RiskScoringJob, after enrichment.
 *
 * KNOWN LIMITATION, inherent to the broadcast state pattern (not something
 * this implementation can fix on its own): updates to the account mapping
 * and events on the main stream are not guaranteed to be processed in a
 * strict global order relative to each other. In practice this means a
 * transaction on a brand-new account could occasionally be scored before
 * its corresponding banking.accounts row has propagated through this join.
 * In that case the event keeps the "acct:<from_account>" fallback identity
 * already set by RiskEventNormalizer rather than being dropped — it's
 * scored under the account instead of the customer for that one event,
 * not silently lost.
 */
public class AccountEnrichmentFunction extends BroadcastProcessFunction<RiskEvent, AccountUpdate, RiskEvent> {
    public static final MapStateDescriptor<String, String> ACCOUNT_STATE_DESCRIPTOR =
        new MapStateDescriptor<>("account-to-customer", String.class, String.class);

    @Override
    public void processElement(RiskEvent event, ReadOnlyContext ctx, Collector<RiskEvent> out) throws Exception {
        if (event == null) {
            return;
        }
        if (event.getCustomerId() == null && event.getFromAccountId() != null) {
            String resolvedCustomerId = ctx.getBroadcastState(ACCOUNT_STATE_DESCRIPTOR).get(event.getFromAccountId());
            if (resolvedCustomerId != null) {
                event.setCustomerId(resolvedCustomerId);
                event.setIdentityKey(resolvedCustomerId);
            }
            // else: mapping not yet known — keep the acct:<from_account>
            // fallback identity already set by RiskEventNormalizer rather
            // than dropping the event.
        }
        out.collect(event);
    }

    @Override
    public void processBroadcastElement(AccountUpdate update, Context ctx, Collector<RiskEvent> out) throws Exception {
        if (update == null || update.getAccountId() == null) {
            return;
        }
        BroadcastState<String, String> state = ctx.getBroadcastState(ACCOUNT_STATE_DESCRIPTOR);
        if (update.isDeleted()) {
            state.remove(update.getAccountId());
        } else if (update.getCustomerId() != null) {
            state.put(update.getAccountId(), update.getCustomerId());
        }
    }
}