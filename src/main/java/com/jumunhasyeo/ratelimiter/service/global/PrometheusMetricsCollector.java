package com.jumunhasyeo.ratelimiter.service.global;

import lombok.Data;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;
import org.springframework.web.client.RestClient;
import org.springframework.web.util.UriUtils;

import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;

@Slf4j
@Service
@RequiredArgsConstructor
public class PrometheusMetricsCollector {

    private final RestClient prometheusRestClient;

    public MetricsSnapshot collectOrderServiceMetrics() {
        try {
            double p95 = getP95Latency();
            double p99 = getP99Latency();
            double connPool = getConnectionPoolUsage();

            log.debug("Metric 데이터 수집: p95={}, p99={}, connPool={}", p95, p99, connPool);

            MetricsSnapshot snapshot = new MetricsSnapshot();
            snapshot.setP95Latency(p95);
            snapshot.setP99Latency(p99);
            snapshot.setConnectionPoolUsage(connPool);
            return snapshot;
        } catch (Exception e) {
            log.error("Prometheus Metrics 수집 실패", e);
            return new MetricsSnapshot();
        }
    }

    private double getP95Latency() {
        String query = """
                histogram_quantile(0.95, sum(rate(http_server_requests_seconds_bucket{service="order"}[1m])) by (le))
                """;
        return queryPrometheus(query);
    }

    private double getP99Latency() {
        String query = """
                histogram_quantile(0.99, sum(rate(http_server_requests_seconds_bucket{service="order"}[1m])) by (le))
                """;
        return queryPrometheus(query);
    }

    private double getConnectionPoolUsage() {
        String query = """
                avg(hikaricp_connections_active{service="order"} / hikaricp_connections_max{service="order"}) * 100
                """;
        return queryPrometheus(query);
    }

    private double queryPrometheus(String query) {
        log.debug("Prometheus 쿼리: {}", query.trim());

        try {
            String encodedQuery = UriUtils.encodeQueryParam(query, StandardCharsets.UTF_8);
            String fullUrl = "/api/v1/query?query=" + encodedQuery;

            PrometheusResponse response = prometheusRestClient.get()
                    .uri(fullUrl)
                    .retrieve()
                    .body(PrometheusResponse.class);

            if (response != null) {
                log.debug("Prometheus 응답: status={}, resultType={}, resultCount={}",
                        response.getStatus(),
                        response.getData() != null ? response.getData().getResultType() : "null",
                        response.getData() != null && response.getData().getResult() != null
                                ? response.getData().getResult().size()
                                : 0
                );

                if (response.getData() != null
                        && response.getData().getResult() != null
                        && !response.getData().getResult().isEmpty()) {

                    String value = response.getData().getResult().get(0).getValue()[1].toString();
                    log.debug("추출된 값: {}", value);
                    return Double.parseDouble(value);
                }
            }

            log.warn("쿼리 응답 없음: {}", query.trim());
            return 0.0;
        } catch (Exception e) {
            log.error("Prometheus 쿼리 실패: {}, error: {}", query.trim(), e.getMessage());
            return 0.0;
        }
    }

    @Data
    public static class MetricsSnapshot {
        private double p95Latency;
        private double p99Latency;
        private double connectionPoolUsage;
    }

    @Data
    private static class PrometheusResponse {
        private String status;
        private PrometheusData data;
    }

    @Data
    private static class PrometheusData {
        private String resultType;
        private List<PrometheusResult> result;
    }

    @Data
    private static class PrometheusResult {
        private Map<String, String> metric;
        private Object[] value;
    }
}
