package com.banking.fraud;

import org.apache.flink.streaming.util.BroadcastOperatorTestHarness;
import org.apache.flink.streaming.util.ProcessFunctionTestHarnesses;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

public class AccountEnrichmentFunctionIntegrationTest {

    @Test
    public void shouldResolveCustomerIdFromAccountsBroadcastForTransactions() throws Exception {
        // Instantiate the test harness for a BroadcastProcessFunction
        BroadcastOperatorTestHarness<RiskEvent, AccountUpdate, RiskEvent> harness = 
            ProcessFunctionTestHarnesses.forBroadcastProcessFunction(
                new AccountEnrichmentFunction(), 
                AccountEnrichmentFunction.ACCOUNT_STATE_DESCRIPTOR
            );

        harness.open();

        // 1. Setup the broadcast mapping data
        AccountUpdate accountMapping = new AccountUpdate("ACC-GEN00007", "cust_gen_7", false);
        
        // 2. Setup the transaction event
        RiskEvent txEvent = new RiskEvent();
        txEvent.setSourceTable("transactions");
        txEvent.setFromAccountId("ACC-GEN00007");
        txEvent.setIdentityKey("acct:ACC-GEN00007");
        txEvent.setEventTime(1_700_000_000_000L);
        txEvent.setDedupKey("evt-1");

        // 3. EXPLICIT ORDERING: Process the broadcast state FIRST
        harness.processBroadcastElement(accountMapping, 1L);
        
        // 4. Process the main stream element SECOND
        harness.processElement(txEvent, 2L);

        // 5. Extract and assert results
        List<RiskEvent> results = harness.extractOutputValues();

        assertEquals(1, results.size());
        RiskEvent enriched = results.get(0);
        assertNotNull(enriched.getCustomerId());
        assertEquals("cust_gen_7", enriched.getCustomerId());
        assertEquals("cust_gen_7", enriched.getIdentityKey());
        
        harness.close();
    }

    @Test
    public void shouldKeepFallbackIdentityWhenAccountMappingIsUnknown() throws Exception {
        BroadcastOperatorTestHarness<RiskEvent, AccountUpdate, RiskEvent> harness = 
            ProcessFunctionTestHarnesses.forBroadcastProcessFunction(
                new AccountEnrichmentFunction(), 
                AccountEnrichmentFunction.ACCOUNT_STATE_DESCRIPTOR
            );

        harness.open();

        RiskEvent txEvent = new RiskEvent();
        txEvent.setSourceTable("transactions");
        txEvent.setFromAccountId("ACC-UNKNOWN-999");
        txEvent.setIdentityKey("acct:ACC-UNKNOWN-999");
        txEvent.setEventTime(1_700_000_000_000L);
        txEvent.setDedupKey("evt-2");

        // Broadcast a completely different account mapping
        harness.processBroadcastElement(new AccountUpdate("ACC-SOMEONE-ELSE", "cust_other", false), 1L);
        
        // Process the transaction for the unknown account
        harness.processElement(txEvent, 2L);

        List<RiskEvent> results = harness.extractOutputValues();

        assertEquals(1, results.size());
        RiskEvent stillFallback = results.get(0);
        
        // Event isn't dropped, and keeps its fallback identity rather than
        // being resolved to the wrong customer.
        assertEquals("acct:ACC-UNKNOWN-999", stillFallback.getIdentityKey());
        
        harness.close();
    }
}