package com.banking.fraud;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class AccountEventNormalizerTest {

    @Test
    public void shouldExtractAccountIdAndCustomerIdOnCreate() {
        String payload = "{\"op\":\"c\",\"after\":{\"account_id\":\"ACC-001\",\"customer_id\":\"cust-1\",\"balance\":5000.00}}";

        AccountUpdate update = AccountEventNormalizer.normalize(payload);

        assertNotNull(update);
        assertEquals("ACC-001", update.getAccountId());
        assertEquals("cust-1", update.getCustomerId());
        assertFalse(update.isDeleted());
    }

    @Test
    public void shouldExtractAccountIdOnUpdate() {
        String payload = "{\"op\":\"u\",\"after\":{\"account_id\":\"ACC-002\",\"customer_id\":\"cust-2\",\"balance\":7500.00,\"status\":\"ACTIVE\"}}";

        AccountUpdate update = AccountEventNormalizer.normalize(payload);

        assertNotNull(update);
        assertEquals("ACC-002", update.getAccountId());
        assertEquals("cust-2", update.getCustomerId());
    }

    @Test
    public void shouldMarkDeletedFromBeforeBlock() {
        String payload = "{\"op\":\"d\",\"after\":null,\"before\":{\"account_id\":\"ACC-003\",\"customer_id\":\"cust-3\"}}";

        AccountUpdate update = AccountEventNormalizer.normalize(payload);

        assertNotNull(update);
        assertEquals("ACC-003", update.getAccountId());
        assertTrue(update.isDeleted());
        assertNull(update.getCustomerId());
    }

    @Test
    public void shouldReturnNullWhenAccountIdMissing() {
        String payload = "{\"op\":\"c\",\"after\":{\"customer_id\":\"cust-4\"}}";
        assertNull(AccountEventNormalizer.normalize(payload));
    }

    @Test
    public void shouldReturnNullOnMalformedJson() {
        assertNull(AccountEventNormalizer.normalize("not json"));
    }
}
