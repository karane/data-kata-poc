package com.poc.source;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.poc.model.SaleEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.InputStream;
import java.net.HttpURLConnection;
import java.net.URL;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

/**
 * Fetches one batch of SaleEvent objects from the sales-api REST endpoint.
 *
 * Designed for BATCH mode: called once before the Flink graph is built so the
 * result can be handed to env.fromCollection(), which is a proper bounded source.
 *
 * Usage:
 *   List<SaleEvent> events = HttpSalesBatchSource.fetchAll("http://sales-api:8080");
 *   DataStream<SaleEvent> fromApi = env.fromCollection(events, TypeInformation.of(SaleEvent.class));
 */
public class HttpSalesBatchSource {

    private static final Logger log = LoggerFactory.getLogger(HttpSalesBatchSource.class);

    private static final ObjectMapper MAPPER = new ObjectMapper()
        .configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);

    private HttpSalesBatchSource() {}

    public static List<SaleEvent> fetchAll(String apiBaseUrl) {
        String endpoint = apiBaseUrl + "/api/sales/events";
        log.info("[HttpSalesBatchSource] Fetching from {}", endpoint);
        try {
            HttpURLConnection conn = (HttpURLConnection) new URL(endpoint).openConnection();
            conn.setConnectTimeout(5_000);
            conn.setReadTimeout(10_000);
            conn.setRequestMethod("GET");

            int status = conn.getResponseCode();
            if (status != 200) {
                log.warn("[HttpSalesBatchSource] HTTP {} from {} — returning empty list", status, endpoint);
                return Collections.emptyList();
            }

            try (InputStream in = conn.getInputStream()) {
                SaleEvent[] batch = MAPPER.readValue(in, SaleEvent[].class);
                log.info("[HttpSalesBatchSource] Received {} events from API", batch.length);
                return Arrays.asList(batch);
            } finally {
                conn.disconnect();
            }
        } catch (Exception e) {
            log.warn("[HttpSalesBatchSource] Failed to fetch from {} — returning empty list: {}", endpoint, e.getMessage());
            return Collections.emptyList();
        }
    }
}
