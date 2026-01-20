package com.jumunhasyeo.ratelimiter.config;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.http.client.SimpleClientHttpRequestFactory;
import org.springframework.web.client.RestClient;

import java.time.Duration;

@Configuration
public class RestClientConfig {

    @Value("${backend.order-service.url}")
    private String orderServiceUrl;

    @Value("${prometheus.url}")
    private String prometheusUrl;

    @Bean
    public RestClient orderServiceRestClient() {
        return RestClient.builder()
                .baseUrl(orderServiceUrl)
                .requestFactory(createRequestFactory())
                .build();
    }

    @Bean
    public RestClient prometheusRestClient() {
        return RestClient.builder()
                .baseUrl(prometheusUrl)
                .requestFactory(createRequestFactory())
                .build();
    }

    private SimpleClientHttpRequestFactory createRequestFactory() {
        SimpleClientHttpRequestFactory factory = new SimpleClientHttpRequestFactory();
        factory.setConnectTimeout(Duration.ofSeconds(5));
        factory.setReadTimeout(Duration.ofSeconds(15));
        return factory;
    }
}
