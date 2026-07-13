package com.banking.fraud;

/**
 * A single account_id -> customer_id mapping event derived from the
 * banking.accounts CDC stream, used to resolve the real customer for
 * transaction events (see AccountEnrichmentFunction).
 */
public class AccountUpdate {
    private String accountId;
    private String customerId;
    private boolean deleted;

    public AccountUpdate() {}

    public AccountUpdate(String accountId, String customerId, boolean deleted) {
        this.accountId = accountId;
        this.customerId = customerId;
        this.deleted = deleted;
    }

    public String getAccountId() { return accountId; }
    public void setAccountId(String accountId) { this.accountId = accountId; }
    public String getCustomerId() { return customerId; }
    public void setCustomerId(String customerId) { this.customerId = customerId; }
    public boolean isDeleted() { return deleted; }
    public void setDeleted(boolean deleted) { this.deleted = deleted; }
}
