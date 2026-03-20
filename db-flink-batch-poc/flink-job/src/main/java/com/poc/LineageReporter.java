package com.poc;

import com.fasterxml.jackson.databind.ObjectMapper;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.util.*;

public class LineageReporter {

    private static final ObjectMapper MAPPER = new ObjectMapper();
    private static HttpClient http;
    private static String atlasBase;
    private static String auth;

    public static void main(String[] args) throws Exception {
        String from          = arg(args, "--from");
        String to            = arg(args, "--to");
        String runId         = arg(args, "--run-id");
        String metricsFile   = arg(args, "--metrics-file");
        if (from == null || to == null || runId == null) {
            throw new IllegalArgumentException("--from, --to and --run-id are required");
        }

        atlasBase = env("ATLAS_URL",  "http://atlas:21000");
        auth      = basicAuth(env("ATLAS_USER", "admin"), env("ATLAS_PASS", "admin"));
        http      = HttpClient.newHttpClient();

        String pgQn = "postgres://"
                    + env("ATLAS_POSTGRES_HOST", "postgres") + ":"
                    + env("ATLAS_POSTGRES_PORT", "5432")     + "/"
                    + env("ATLAS_POSTGRES_DB",   "salesdb");

        String s3Qn = "s3a://sales-csv@"
                    + env("ATLAS_RUSTFS_HOST", "rustfs") + ":"
                    + env("ATLAS_RUSTFS_PORT", "9000");

        String apiHostPort = env("ATLAS_SALES_API_HOST", "sales-api")
                           + ":" + env("ATLAS_SALES_API_PORT", "8080");
        String apiQn  = "http://" + apiHostPort + "/api/sales/events#GET";
        String apiUrl = "http://" + apiHostPort + "/api/sales/events";

        JobMetrics metrics = null;
        if (metricsFile != null) {
            metrics = MAPPER.readValue(new java.io.File(metricsFile), JobMetrics.class);
        }

        ensureTypes();
        upsertDatasets(pgQn, s3Qn, apiQn, apiUrl);
        upsertViewLineage(pgQn);
        createProcess(from, to, runId, pgQn, s3Qn, apiQn, metrics);

        System.out.println("[LineageReporter] Lineage recorded: run=" + runId
            + "  from=" + from + "  to=" + to);
    }

    private static void ensureTypes() throws Exception {
        Map<String, Object> httpDef = simpleDef("http_endpoint");
        httpDef.put("attributeDefs", List.of(
            attrDef("url",    "string", false),
            attrDef("method", "string", true)
        ));

        Map<String, Object> flinkJobDef = new LinkedHashMap<>();
        flinkJobDef.put("name",          "poc_flink_job");
        flinkJobDef.put("superTypes",    List.of("Process"));
        flinkJobDef.put("serviceType",   "poc");
        flinkJobDef.put("typeVersion",   "1.0");
        flinkJobDef.put("attributeDefs", List.of(
            attrDef("recordsFromDb",  "long", true),
            attrDef("recordsFromS3",  "long", true),
            attrDef("recordsFromApi", "long", true),
            attrDef("recordsWritten", "long", true),
            attrDef("durationMs",     "long", true)
        ));

        int s = post("/api/atlas/v2/types/typedefs", Map.of("entityDefs", List.of(
            simpleDef("poc_db_table"),
            simpleDef("poc_s3_bucket"),
            httpDef,
            flinkJobDef
        )));
        if (s == 200 || s == 201) return;

        if (s == 409) {
            // Batch rejected because some types already exist; register poc_flink_job individually
            int s2 = post("/api/atlas/v2/types/typedefs", Map.of("entityDefs", List.of(flinkJobDef)));
            if (s2 != 200 && s2 != 201 && s2 != 409) {
                throw new RuntimeException("Failed to register poc_flink_job type, HTTP " + s2);
            }
            return;
        }

        throw new RuntimeException("Failed to register custom types, HTTP " + s);
    }

    private static void upsertDatasets(String pgQn, String s3Qn,
                                       String apiQn, String apiUrl) throws Exception {
        Map<String, Object> apiAttrs = new LinkedHashMap<>();
        apiAttrs.put("qualifiedName", apiQn);
        apiAttrs.put("name",   "sales-api /api/sales/events");
        apiAttrs.put("url",    apiUrl);
        apiAttrs.put("method", "GET");

        List<Map<String, Object>> entities = List.of(
            dbTable(pgQn + "/source_sales",       "source_sales",       "Raw sales events in PostgreSQL"),
            dbTable(pgQn + "/sales_ranks",         "sales_ranks",         "Aggregated city and salesman sales rankings"),
            dbTable(pgQn + "/top_cities_latest",   "top_cities_latest",   "View: top cities by total_sales per window"),
            dbTable(pgQn + "/top_salesmen_latest", "top_salesmen_latest", "View: top salesmen by total_sales per window"),
            Map.of("typeName", "poc_s3_bucket",
                "attributes", Map.of("qualifiedName", s3Qn, "name", "sales-csv")),
            Map.of("typeName", "http_endpoint", "attributes", apiAttrs)
        );

        int s = post("/api/atlas/v2/entity/bulk", Map.of("entities", entities));
        if (s != 200 && s != 201) {
            throw new RuntimeException("Failed to upsert dataset entities, HTTP " + s);
        }
    }

    private static void upsertViewLineage(String pgQn) throws Exception {
        List<Map<String, Object>> processes = new ArrayList<>();
        for (String view : new String[]{"top_cities_latest", "top_salesmen_latest"}) {
            Map<String, Object> attrs = new LinkedHashMap<>();
            attrs.put("qualifiedName", pgQn + "/view/" + view);
            attrs.put("name",          "PostgreSQL VIEW: " + view);
            attrs.put("operationType", "sql-view");
            attrs.put("inputs",  List.of(ref("poc_db_table", pgQn + "/sales_ranks")));
            attrs.put("outputs", List.of(ref("poc_db_table", pgQn + "/" + view)));
            processes.add(Map.of("typeName", "Process", "attributes", attrs));
        }
        int s = post("/api/atlas/v2/entity/bulk", Map.of("entities", processes));
        if (s != 200 && s != 201) {
            throw new RuntimeException("Failed to upsert view lineage, HTTP " + s);
        }
    }

    private static void createProcess(String from, String to, String runId,
                                      String pgQn, String s3Qn, String apiQn,
                                      JobMetrics metrics) throws Exception {
        String qn = "flink://batch-job/sales-ranking/"
                  + from.replace("-", "") + "_" + to.replace("-", "") + "/" + runId;

        Map<String, Object> attrs = new LinkedHashMap<>();
        attrs.put("qualifiedName", qn);
        attrs.put("name",          "Flink BatchJob: sales-ranking " + from + " -> " + to);
        attrs.put("operationType", "flink-batch");
        attrs.put("inputs", List.of(
            ref("poc_db_table",  pgQn + "/source_sales"),
            ref("poc_s3_bucket", s3Qn),
            ref("http_endpoint", apiQn)
        ));
        attrs.put("outputs", List.of(
            ref("poc_db_table", pgQn + "/sales_ranks")
        ));
        if (metrics != null) {
            attrs.put("recordsFromDb",  metrics.recordsFromDb);
            attrs.put("recordsFromS3",  metrics.recordsFromS3);
            attrs.put("recordsFromApi", metrics.recordsFromApi);
            attrs.put("recordsWritten", metrics.recordsWritten);
            attrs.put("durationMs",     metrics.durationMs);
        }

        int s = post("/api/atlas/v2/entity/bulk",
            Map.of("entities", List.of(Map.of("typeName", "poc_flink_job", "attributes", attrs))));
        if (s != 200 && s != 201) {
            throw new RuntimeException("Failed to create Process entity, HTTP " + s);
        }
    }

    // ── Builders ──────────────────────────────────────────────────────────────

    private static Map<String, Object> simpleDef(String name) {
        Map<String, Object> m = new LinkedHashMap<>();
        m.put("name",          name);
        m.put("superTypes",    List.of("DataSet"));
        m.put("serviceType",   "poc");
        m.put("typeVersion",   "1.0");
        m.put("attributeDefs", new ArrayList<>());
        return m;
    }

    private static Map<String, Object> attrDef(String name, String typeName, boolean optional) {
        Map<String, Object> m = new LinkedHashMap<>();
        m.put("name",        name);
        m.put("typeName",    typeName);
        m.put("isOptional",  optional);
        m.put("cardinality", "SINGLE");
        m.put("isUnique",    false);
        m.put("isIndexable", false);
        return m;
    }

    private static Map<String, Object> dbTable(String qn, String name, String description) {
        Map<String, Object> attrs = new LinkedHashMap<>();
        attrs.put("qualifiedName", qn);
        attrs.put("name",          name);
        attrs.put("description",   description);
        return Map.of("typeName", "poc_db_table", "attributes", attrs);
    }

    private static Map<String, Object> ref(String typeName, String qn) {
        return Map.of("typeName", typeName, "uniqueAttributes", Map.of("qualifiedName", qn));
    }

    // ── HTTP ──────────────────────────────────────────────────────────────────

    private static int post(String path, Object body) throws Exception {
        String json = MAPPER.writeValueAsString(body);
        HttpRequest req = HttpRequest.newBuilder()
            .uri(URI.create(atlasBase + path))
            .header("Content-Type", "application/json")
            .header("Authorization", auth)
            .POST(HttpRequest.BodyPublishers.ofString(json))
            .build();
        return http.send(req, HttpResponse.BodyHandlers.discarding()).statusCode();
    }

    private static int put(String path, Object body) throws Exception {
        String json = MAPPER.writeValueAsString(body);
        HttpRequest req = HttpRequest.newBuilder()
            .uri(URI.create(atlasBase + path))
            .header("Content-Type", "application/json")
            .header("Authorization", auth)
            .PUT(HttpRequest.BodyPublishers.ofString(json))
            .build();
        return http.send(req, HttpResponse.BodyHandlers.discarding()).statusCode();
    }

    // ── Helpers ───────────────────────────────────────────────────────────────

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

    private static String basicAuth(String user, String pass) {
        return "Basic " + Base64.getEncoder().encodeToString((user + ":" + pass).getBytes());
    }

    private static class JobMetrics {
        public long recordsFromDb;
        public long recordsFromS3;
        public long recordsFromApi;
        public long recordsWritten;
        public long durationMs;
    }
}
