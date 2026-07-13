package com.banking.fraud;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

public class RiskProfileState {
    private static final int MAX_RETAINED_TIMESTAMPS = 50;

    private int cumulativeScore;
    private long lastScoreUpdateTs;
    private long lastAlertTs;
    private final List<String> activeIndicators = new ArrayList<>();
    private final Map<String, Integer> indicatorCounts = new LinkedHashMap<>();
    private String lastKnownCountry;
    private String lastKnownDeviceId;
    private int recentLoginCount;
    private int velocityWindowCounter;
    private String profileStatus = "ACTIVE";

    // NEW — support for indicator derivation (bounded ring buffers, not
    // unbounded audit logs — see recordAndPrune).
    private final List<Long> failedLoginTimestamps = new ArrayList<>();
    private final List<Long> transactionTimestamps = new ArrayList<>();
    private final List<Long> apiRequestTimestamps = new ArrayList<>();
    // CHANGED: was lastSessionCountry/lastSessionTs. The real Session model
    // has no country/geo column, so IMPOSSIBLE_TRAVEL can't be derived from
    // sessions — it's derived from consecutive login_events country changes
    // instead, timed against lastLoginEventTs.
    private long lastLoginEventTs;

    // NEW — score decay bookkeeping.
    private long lastDecayAppliedTs;
    private long nextDecayTimerTs;

    public int getCumulativeScore() { return cumulativeScore; }
    public void setCumulativeScore(int cumulativeScore) { this.cumulativeScore = Math.max(0, cumulativeScore); }

    public long getLastScoreUpdateTs() { return lastScoreUpdateTs; }
    public void setLastScoreUpdateTs(long lastScoreUpdateTs) { this.lastScoreUpdateTs = lastScoreUpdateTs; }

    public long getLastAlertTs() { return lastAlertTs; }
    public void setLastAlertTs(long lastAlertTs) { this.lastAlertTs = lastAlertTs; }

    public List<String> getActiveIndicators() { return activeIndicators; }
    public Map<String, Integer> getIndicatorCounts() { return indicatorCounts; }

    public String getLastKnownCountry() { return lastKnownCountry; }
    public void setLastKnownCountry(String lastKnownCountry) { this.lastKnownCountry = lastKnownCountry; }

    public String getLastKnownDeviceId() { return lastKnownDeviceId; }
    public void setLastKnownDeviceId(String lastKnownDeviceId) { this.lastKnownDeviceId = lastKnownDeviceId; }

    public int getRecentLoginCount() { return recentLoginCount; }
    public void setRecentLoginCount(int recentLoginCount) { this.recentLoginCount = recentLoginCount; }

    public int getVelocityWindowCounter() { return velocityWindowCounter; }
    public void setVelocityWindowCounter(int velocityWindowCounter) { this.velocityWindowCounter = velocityWindowCounter; }

    public String getProfileStatus() { return profileStatus; }
    public void setProfileStatus(String profileStatus) { this.profileStatus = profileStatus; }

    public long getLastLoginEventTs() { return lastLoginEventTs; }
    public void setLastLoginEventTs(long lastLoginEventTs) { this.lastLoginEventTs = lastLoginEventTs; }

    public long getLastDecayAppliedTs() { return lastDecayAppliedTs; }
    public void setLastDecayAppliedTs(long lastDecayAppliedTs) { this.lastDecayAppliedTs = lastDecayAppliedTs; }

    public long getNextDecayTimerTs() { return nextDecayTimerTs; }
    public void setNextDecayTimerTs(long nextDecayTimerTs) { this.nextDecayTimerTs = nextDecayTimerTs; }

    public void addIndicator(String indicator) {
        activeIndicators.add(indicator);
        if (activeIndicators.size() > 8) {
            activeIndicators.remove(0);
        }
        indicatorCounts.merge(indicator, 1, Integer::sum);
    }

    /** Records a failed-login timestamp and returns the count within the window. */
    public int recordFailedLogin(long now, long windowMs) {
        return recordAndPrune(failedLoginTimestamps, now, windowMs);
    }

    public void clearFailedLogins() {
        failedLoginTimestamps.clear();
    }

    /** Records a transaction timestamp and returns the count within the window. */
    public int recordTransaction(long now, long windowMs) {
        return recordAndPrune(transactionTimestamps, now, windowMs);
    }

    /** Records an api_requests timestamp and returns the count within the window. */
    public int recordApiRequest(long now, long windowMs) {
        return recordAndPrune(apiRequestTimestamps, now, windowMs);
    }

    /**
     * Appends `now`, drops anything older than the window, and hard-caps the
     * list at MAX_RETAINED_TIMESTAMPS regardless of window size — this is
     * the lean-state guarantee: these lists can never grow unbounded even
     * under a pathological event burst.
     */
    private static int recordAndPrune(List<Long> timestamps, long now, long windowMs) {
        timestamps.add(now);
        timestamps.removeIf(ts -> now - ts > windowMs);
        if (timestamps.size() > MAX_RETAINED_TIMESTAMPS) {
            timestamps.subList(0, timestamps.size() - MAX_RETAINED_TIMESTAMPS).clear();
        }
        return timestamps.size();
    }
}