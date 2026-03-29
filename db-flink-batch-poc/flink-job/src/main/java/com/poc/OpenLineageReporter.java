package com.poc;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.UUID;

/**
 * Emits OpenLineage RunEvent JSON to Marquez after the Flink batch job completes.
 *
 * Invoked as a standalone main class (same pattern as LineageReporter for Atlas):
 *   java -cp flink-job.jar com.poc.OpenLineageReporter \
 *       --from 2024-02-01 --to 2024-02-29 --run-id 123456 --metrics-file /tmp/job-metrics.json
 */
public class OpenLineageReporter {

    private static final ObjectMapper MAPPER = new ObjectMapper();
    private static final String PRODUCER = "https://github.com/karane/data-kata-poc/flink-batch-job";
    private static final String SCHEMA_URL = "https://openlineage.io/spec/2-0-2/OpenLineage.json#/$defs/RunEvent";
    private static final String SCHEMA_FACET_URL = "https://openlineage.io/spec/facets/1-1-1/SchemaDatasetFacet.json#/$defs/SchemaDatasetFacet";

    public static void main(String[] args) throws Exception {
        String from        = arg(args, "--from");
        String to          = arg(args, "--to");
        String runId       = arg(args, "--run-id");
        String metricsFile = arg(args, "--metrics-file");
        if (from == null || to == null || runId == null) {
            throw new IllegalArgumentException("--from, --to and --run-id are required");
        }

        String marquezUrl = env("MARQUEZ_URL", "http://marquez:5000");
        String namespace  = env("MARQUEZ_NAMESPACE", "sales-batch-poc");
        String jobName    = "sales-ranking-batch-job";

        JobMetrics metrics = null;
        if (metricsFile != null) {
            metrics = MAPPER.readValue(new java.io.File(metricsFile), JobMetrics.class);
        }

        UUID runUuid = UUID.nameUUIDFromBytes(("run-" + runId).getBytes());

        // Send START event
        ObjectNode startEvent = buildRunEvent("START", runUuid, namespace, jobName, from, to, null);
        postLineage(marquezUrl, startEvent);

        // Send COMPLETE event with metrics
        ObjectNode completeEvent = buildRunEvent("COMPLETE", runUuid, namespace, jobName, from, to, metrics);
        postLineage(marquezUrl, completeEvent);

        System.out.println("[OpenLineageReporter] Lineage sent to Marquez: run=" + runUuid
            + "  from=" + from + "  to=" + to);
    }

    private static ObjectNode buildRunEvent(String eventType, UUID runId, String namespace,
                                             String jobName, String from, String to,
                                             JobMetrics metrics) {
        ObjectNode event = MAPPER.createObjectNode();
        event.put("eventType", eventType);
        event.put("eventTime", ZonedDateTime.now(ZoneOffset.UTC)
            .format(DateTimeFormatter.ISO_OFFSET_DATE_TIME));
        event.put("producer", PRODUCER);
        event.put("schemaURL", SCHEMA_URL);

        // -- Run
        ObjectNode run = MAPPER.createObjectNode();
        run.put("runId", runId.toString());
        ObjectNode runFacets = MAPPER.createObjectNode();

        ObjectNode processingRange = MAPPER.createObjectNode();
        processingRange.put("_producer", PRODUCER);
        processingRange.put("_schemaURL", SCHEMA_URL);
        processingRange.put("fromDate", from);
        processingRange.put("toDate", to);
        runFacets.set("processing_dateRange", processingRange);

        if (metrics != null) {
            ObjectNode metricsFacet = MAPPER.createObjectNode();
            metricsFacet.put("_producer", PRODUCER);
            metricsFacet.put("_schemaURL", SCHEMA_URL);
            metricsFacet.put("recordsFromDb", metrics.recordsFromDb);
            metricsFacet.put("recordsFromS3", metrics.recordsFromS3);
            metricsFacet.put("recordsFromApi", metrics.recordsFromApi);
            metricsFacet.put("recordsWritten", metrics.recordsWritten);
            metricsFacet.put("durationMs", metrics.durationMs);
            runFacets.set("processing_metrics", metricsFacet);
        }
        run.set("facets", runFacets);
        event.set("run", run);

        // -- Job
        ObjectNode job = MAPPER.createObjectNode();
        job.put("namespace", namespace);
        job.put("name", jobName);
        event.set("job", job);

        // -- Inputs
        ArrayNode inputs = MAPPER.createArrayNode();
        inputs.add(datasetNode("postgres-source", "salesdb.public.source_sales", sourceSchemaFacet()));
        inputs.add(datasetNode("s3-rustfs", "sales-csv", null));
        inputs.add(datasetNode("http-api", "sales-api.sales-events", null));
        event.set("inputs", inputs);

        // -- Outputs
        ArrayNode outputs = MAPPER.createArrayNode();
        outputs.add(datasetNode("postgres-sink", "salesdb.public.sales_ranks", sinkSchemaFacet()));
        event.set("outputs", outputs);

        return event;
    }

    private static ObjectNode datasetNode(String namespace, String name, ObjectNode schemaFacet) {
        ObjectNode ds = MAPPER.createObjectNode();
        ds.put("namespace", namespace);
        ds.put("name", name);
        if (schemaFacet != null) {
            ObjectNode facets = MAPPER.createObjectNode();
            facets.set("schema", schemaFacet);
            ds.set("facets", facets);
        }
        return ds;
    }

    private static ObjectNode sourceSchemaFacet() {
        ObjectNode schema = MAPPER.createObjectNode();
        schema.put("_producer", PRODUCER);
        schema.put("_schemaURL", SCHEMA_FACET_URL);
        ArrayNode fields = MAPPER.createArrayNode();
        for (String[] f : new String[][]{
            {"sale_id", "VARCHAR"}, {"salesman_id", "VARCHAR"}, {"salesman_name", "VARCHAR"},
            {"city", "VARCHAR"}, {"region", "VARCHAR"}, {"product_id", "VARCHAR"},
            {"amount", "DECIMAL"}, {"event_time", "BIGINT"}}) {
            ObjectNode field = MAPPER.createObjectNode();
            field.put("name", f[0]);
            field.put("type", f[1]);
            fields.add(field);
        }
        schema.set("fields", fields);
        return schema;
    }

    private static ObjectNode sinkSchemaFacet() {
        ObjectNode schema = MAPPER.createObjectNode();
        schema.put("_producer", PRODUCER);
        schema.put("_schemaURL", SCHEMA_FACET_URL);
        ArrayNode fields = MAPPER.createArrayNode();
        for (String[] f : new String[][]{
            {"rank_type", "VARCHAR"}, {"group_key", "VARCHAR"}, {"entity_id", "VARCHAR"},
            {"total_sales", "DECIMAL"}, {"window_start", "TIMESTAMP"}, {"window_end", "TIMESTAMP"}}) {
            ObjectNode field = MAPPER.createObjectNode();
            field.put("name", f[0]);
            field.put("type", f[1]);
            fields.add(field);
        }
        schema.set("fields", fields);
        return schema;
    }

    private static void postLineage(String marquezUrl, ObjectNode event) throws Exception {
        String json = MAPPER.writerWithDefaultPrettyPrinter().writeValueAsString(event);
        HttpRequest req = HttpRequest.newBuilder()
            .uri(URI.create(marquezUrl + "/api/v1/lineage"))
            .header("Content-Type", "application/json")
            .POST(HttpRequest.BodyPublishers.ofString(json))
            .build();
        HttpResponse<String> resp = HttpClient.newHttpClient()
            .send(req, HttpResponse.BodyHandlers.ofString());
        if (resp.statusCode() != 200 && resp.statusCode() != 201) {
            System.err.println("[OpenLineageReporter] Marquez response: " + resp.body());
            throw new RuntimeException("Marquez rejected lineage event, HTTP " + resp.statusCode());
        }
    }

    private static String arg(String[] args, String key) {
        for (int i = 0; i < args.length - 1; i++) {
            if (key.equals(args[i])) return args[i + 1];
        }
        return null;
    }

    private static String env(String key, String def) {
        String v = System.getenv(key);
        return (v != null && !v.isEmpty()) ? v : def;
    }

    static class JobMetrics {
        public long recordsFromDb;
        public long recordsFromS3;
        public long recordsFromApi;
        public long recordsWritten;
        public long durationMs;
    }
}
