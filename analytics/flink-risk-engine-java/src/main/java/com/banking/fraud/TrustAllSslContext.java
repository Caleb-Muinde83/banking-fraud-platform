package com.banking.fraud;

import javax.net.ssl.SSLContext;
import javax.net.ssl.TrustManager;
import javax.net.ssl.X509TrustManager;
import java.security.SecureRandom;
import java.security.cert.X509Certificate;

/**
 * Trust-all SSLContext for talking to OpenSearch's self-signed demo certs
 * from inside the Flink job -- same verify_certs=False posture the Python
 * opensearch_sink.py already uses against this exact same OpenSearch
 * instance. Fine for this homelab setup; would need real certificate
 * validation before any production use.
 */
final class TrustAllSslContext {
    private TrustAllSslContext() {}

    static SSLContext get() {
        try {
            TrustManager[] trustAll = new TrustManager[]{
                new X509TrustManager() {
                    public X509Certificate[] getAcceptedIssuers() { return new X509Certificate[0]; }
                    public void checkClientTrusted(X509Certificate[] certs, String authType) {}
                    public void checkServerTrusted(X509Certificate[] certs, String authType) {}
                }
            };
            SSLContext ctx = SSLContext.getInstance("TLS");
            ctx.init(null, trustAll, new SecureRandom());
            return ctx;
        } catch (Exception e) {
            throw new IllegalStateException("Failed to build trust-all SSLContext", e);
        }
    }
}