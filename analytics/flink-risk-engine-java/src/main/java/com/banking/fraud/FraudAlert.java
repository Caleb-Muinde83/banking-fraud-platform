package com.banking.fraud;

import java.util.List;

public class FraudAlert {
    private String alertId;
    private String customerId;
    private String accountId;
    private long eventTime;
    private String severity;
    private int cumulativeScore;
    private int threshold;
    private List<String> contributingIndicators;
    private String triggerReason;
    private long firstSeenTs;
    private long latestSeenTs;
    private List<String> sourceEventIds;
    private String actionRequired;

    public FraudAlert() {}

    public FraudAlert(String alertId, String customerId, String accountId, long eventTime, String severity,
                      int cumulativeScore, int threshold, List<String> contributingIndicators,
                      String triggerReason, long firstSeenTs, long latestSeenTs, List<String> sourceEventIds,
                      String actionRequired) {
        this.alertId = alertId;
        this.customerId = customerId;
        this.accountId = accountId;
        this.eventTime = eventTime;
        this.severity = severity;
        this.cumulativeScore = cumulativeScore;
        this.threshold = threshold;
        this.contributingIndicators = contributingIndicators;
        this.triggerReason = triggerReason;
        this.firstSeenTs = firstSeenTs;
        this.latestSeenTs = latestSeenTs;
        this.sourceEventIds = sourceEventIds;
        this.actionRequired = actionRequired;
    }

    public String getAlertId() { return alertId; }
    public void setAlertId(String alertId) { this.alertId = alertId; }
    public String getCustomerId() { return customerId; }
    public void setCustomerId(String customerId) { this.customerId = customerId; }
    public String getAccountId() { return accountId; }
    public void setAccountId(String accountId) { this.accountId = accountId; }
    public long getEventTime() { return eventTime; }
    public void setEventTime(long eventTime) { this.eventTime = eventTime; }
    public String getSeverity() { return severity; }
    public void setSeverity(String severity) { this.severity = severity; }
    public int getCumulativeScore() { return cumulativeScore; }
    public void setCumulativeScore(int cumulativeScore) { this.cumulativeScore = cumulativeScore; }
    public int getThreshold() { return threshold; }
    public void setThreshold(int threshold) { this.threshold = threshold; }
    public List<String> getContributingIndicators() { return contributingIndicators; }
    public void setContributingIndicators(List<String> contributingIndicators) { this.contributingIndicators = contributingIndicators; }
    public String getTriggerReason() { return triggerReason; }
    public void setTriggerReason(String triggerReason) { this.triggerReason = triggerReason; }
    public long getFirstSeenTs() { return firstSeenTs; }
    public void setFirstSeenTs(long firstSeenTs) { this.firstSeenTs = firstSeenTs; }
    public long getLatestSeenTs() { return latestSeenTs; }
    public void setLatestSeenTs(long latestSeenTs) { this.latestSeenTs = latestSeenTs; }
    public List<String> getSourceEventIds() { return sourceEventIds; }
    public void setSourceEventIds(List<String> sourceEventIds) { this.sourceEventIds = sourceEventIds; }
    public String getActionRequired() { return actionRequired; }
    public void setActionRequired(String actionRequired) { this.actionRequired = actionRequired; }
}
