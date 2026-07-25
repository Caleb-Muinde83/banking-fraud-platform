package com.banking.fraud;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.async.ResultFuture;
import org.apache.flink.streaming.api.functions.async.RichAsyncFunction;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.Base64;
import java.util.Collections;
import java.util.concurrent.TimeUnit;

/**
 * Async I/O lookup against OpenSearch: for each RiskEvent, asks "has this
 * identityKey appeared in the login_events index at any point BEFORE the
 * last RiskScoringConfig.TTL_MS (24h)?" -- deliberately outside Flink's own
 * state TTL window, since anything more recent is already covered for free
 * by RiskProfileState. This is purely additive long-term context
 * (days/weeks of history), not a replacement for the existing in-state
 * indicators, and its result (seenBeyondStateWindow) is NOT currently wired
 * into deriveIndicators()/scoring -- that's a deliberate follow-up decision
 * left for later, not an oversight, since it changes real alerting
 * behavior and shouldn't happen as a side effect of adding this pattern.
 *
 * Uses Java 11's built-in java.net.http.HttpClient with sendAsync() --
 * genuinely non-blocking, no new Maven dependency needed (pom.xml already
 * targets Java 11 and already depends on jackson-databind, reused here for
 * parsing OpenSearch's response).
 */
public class OpenSearchLookupFunction extends RichAsyncFunction<RiskEvent, RiskEvent> {

    private static final long serialVersionUID = 1L;
    private static final ObjectMapper MAPPER = new ObjectMapper();

    private transient HttpClient httpClient;
    private transient String openSearchCountUrl;
    private transient String authHeader;

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        String host = System.getenv().getOrDefault("OPENSEARCH_HOST", "opensearch");
        String port = System.getenv().getOrDefault("OPENSEARCH_PORT", "9200");
        String user = System.getenv().getOrDefault("OPENSEARCH_USER", "admin");
        String pass = System.getenv().getOrDefault("OPENSEARCH_PASSWORD", "admin");

        // _count is used instead of _search: we only need "does at least
        // one document exist older than the cutoff", not the documents
        // themselves -- cheaper for OpenSearch to compute and cheaper to
        // parse, which matters given this runs per-event.
        this.openSearchCountUrl = "https://" + host + ":" + port + "/login_events/_count";
        this.authHeader = "Basic " + Base64.getEncoder().encodeToString(
            (user + ":" + pass).getBytes(StandardCharsets.UTF_8));

        this.httpClient = HttpClient.newBuilder()
            .connectTimeout(Duration.ofSeconds(3))
            .sslContext(TrustAllSslContext.get())
            .build();
    }

    @Override
    public void asyncInvoke(RiskEvent event, ResultFuture<RiskEvent> resultFuture) {
        if (event.getIdentityKey() == null) {
            resultFuture.complete(Collections.singletonList(event));
            return;
        }

        long cutoffMs = event.getEventTime() - RiskScoringConfig.TTL_MS;
        // Basic sanitization -- identityKey can contain characters like
        // "ip:" prefixes; stripping quotes is enough to keep this JSON
        // body well-formed without needing a full JSON-building library
        // for one field.
        String safeIdentity = event.getIdentityKey().replace("\"", "");

        String body = "{\"query\":{\"bool\":{\"filter\":["
            + "{\"term\":{\"customer_id\":\"" + safeIdentity + "\"}},"
            + "{\"range\":{\"timestamp\":{\"lt\":" + cutoffMs + "}}}"
            + "]}}}";

        HttpRequest request = HttpRequest.newBuilder()
            .uri(URI.create(openSearchCountUrl))
            .header("Content-Type", "application/json")
            .header("Authorization", authHeader)
            .timeout(Duration.ofSeconds(5))
            .POST(HttpRequest.BodyPublishers.ofString(body))
            .build();

        httpClient.sendAsync(request, HttpResponse.BodyHandlers.ofString())
            .orTimeout(5, TimeUnit.SECONDS)
            .whenComplete((response, error) -> {
                if (error != null) {
                    // Fail open, deliberately: an analytics-tier dependency
                    // (OpenSearch) being slow or unreachable must never
                    // block the real-time scoring path. Matches the Python
                    // sink's own fail-open-to-DLQ posture against this
                    // exact same OpenSearch instance.
                    event.setSeenBeyondStateWindow(null);
                    resultFuture.complete(Collections.singletonList(event));
                    return;
                }
                try {
                    JsonNode node = MAPPER.readTree(response.body());
                    long count = node.path("count").asLong(0L);
                    event.setSeenBeyondStateWindow(count > 0);
                } catch (Exception parseError) {
                    event.setSeenBeyondStateWindow(null);
                }
                resultFuture.complete(Collections.singletonList(event));
            });
    }

    @Override
    public void timeout(RiskEvent event, ResultFuture<RiskEvent> resultFuture) {
        // Same fail-open reasoning as the error branch in asyncInvoke.
        event.setSeenBeyondStateWindow(null);
        resultFuture.complete(Collections.singletonList(event));
    }
}