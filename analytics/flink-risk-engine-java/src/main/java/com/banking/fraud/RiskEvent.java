package com.banking.fraud;

import java.util.Map;

public class RiskEvent {
    private String eventId;
    private String customerId;
    private String accountId;
    // NOTE: retained for back-compat / explicit-override use (e.g. tests, or
    // an upstream source that already labels its own indicator). When null,
    // RiskScoringProcessor derives indicators itself from the raw fields below.
    private String indicator;
    private long eventTime;
    private String sourceTopic;
    private String sourceOperation;
    private String dedupKey;
    private Map<String, Object> payload;

    // NEW — raw fields needed to derive indicators (previously absent; the
    // engine only ever consumed a pre-labeled `indicator`).
    private String sourceTable;
    private String country;
    private String deviceId;
    private Boolean loginSuccess;
    private Double amount;
    private String beneficiaryId;
    private String ipAddress;
    // NEW — confirmed against the real Transaction model: it has
    // from_account/to_account, NOT customer_id or beneficiary_id. These
    // replace the earlier (wrong) assumption that transactions carried a
    // beneficiary_id column directly.
    private String fromAccountId;
    private String toAccountId;
    // Key used for keyBy(). customer_id when present; falls back to
    // "ip:<address>" for sources like api_requests that carry no customer_id,
    // or "acct:<from_account>" for transactions until a proper account_id ->
    // customer_id join (via banking.accounts) is added — see normalizer.
    private String identityKey;

    // NEW — populated by OpenSearchLookupFunction (Async I/O practice
    // implementation). Boolean, not boolean, on purpose: null means "not
    // looked up yet, or the lookup failed/timed out" (fail-open), distinct
    // from a real false meaning "looked up, genuinely never seen before
    // the state window". Not currently read anywhere in scoring logic --
    // deliberately left as a follow-up decision, not wired in yet.
    private Boolean seenBeyondStateWindow;

    public RiskEvent() {}

    public RiskEvent(String eventId, String customerId, String accountId, String indicator, long eventTime,
                     String sourceTopic, String sourceOperation, String dedupKey, Map<String, Object> payload) {
        this.eventId = eventId;
        this.customerId = customerId;
        this.accountId = accountId;
        this.indicator = indicator;
        this.eventTime = eventTime;
        this.sourceTopic = sourceTopic;
        this.sourceOperation = sourceOperation;
        this.dedupKey = dedupKey;
        this.payload = payload;
    }

    public String getEventId() { return eventId; }
    public void setEventId(String eventId) { this.eventId = eventId; }
    public String getCustomerId() { return customerId; }
    public void setCustomerId(String customerId) { this.customerId = customerId; }
    public String getAccountId() { return accountId; }
    public void setAccountId(String accountId) { this.accountId = accountId; }
    public String getIndicator() { return indicator; }
    public void setIndicator(String indicator) { this.indicator = indicator; }
    public long getEventTime() { return eventTime; }
    public void setEventTime(long eventTime) { this.eventTime = eventTime; }
    public String getSourceTopic() { return sourceTopic; }
    public void setSourceTopic(String sourceTopic) { this.sourceTopic = sourceTopic; }
    public String getSourceOperation() { return sourceOperation; }
    public void setSourceOperation(String sourceOperation) { this.sourceOperation = sourceOperation; }
    public String getDedupKey() { return dedupKey; }
    public void setDedupKey(String dedupKey) { this.dedupKey = dedupKey; }
    public Map<String, Object> getPayload() { return payload; }
    public void setPayload(Map<String, Object> payload) { this.payload = payload; }

    public String getSourceTable() { return sourceTable; }
    public void setSourceTable(String sourceTable) { this.sourceTable = sourceTable; }
    public String getCountry() { return country; }
    public void setCountry(String country) { this.country = country; }
    public String getDeviceId() { return deviceId; }
    public void setDeviceId(String deviceId) { this.deviceId = deviceId; }
    public Boolean getLoginSuccess() { return loginSuccess; }
    public void setLoginSuccess(Boolean loginSuccess) { this.loginSuccess = loginSuccess; }
    public Double getAmount() { return amount; }
    public void setAmount(Double amount) { this.amount = amount; }
    public String getBeneficiaryId() { return beneficiaryId; }
    public void setBeneficiaryId(String beneficiaryId) { this.beneficiaryId = beneficiaryId; }
    public String getIpAddress() { return ipAddress; }
    public void setIpAddress(String ipAddress) { this.ipAddress = ipAddress; }
    public String getFromAccountId() { return fromAccountId; }
    public void setFromAccountId(String fromAccountId) { this.fromAccountId = fromAccountId; }
    public String getToAccountId() { return toAccountId; }
    public void setToAccountId(String toAccountId) { this.toAccountId = toAccountId; }
    public String getIdentityKey() { return identityKey; }
    public void setIdentityKey(String identityKey) { this.identityKey = identityKey; }
    public Boolean getSeenBeyondStateWindow() { return seenBeyondStateWindow; }
    public void setSeenBeyondStateWindow(Boolean seenBeyondStateWindow) { this.seenBeyondStateWindow = seenBeyondStateWindow; }
}