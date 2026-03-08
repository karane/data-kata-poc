package com.poc;

import com.poc.BatchJob.CityAcc;
import com.poc.BatchJob.SalesmanAcc;
import com.poc.model.SaleEvent;
import com.poc.model.SalesRank;
import com.poc.source.CsvSaleEventFormat;
import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.connector.file.src.FileSource;
import org.apache.flink.connector.jdbc.JdbcConnectionOptions;
import org.apache.flink.connector.jdbc.JdbcExecutionOptions;
import org.apache.flink.connector.jdbc.JdbcSink;
import org.apache.flink.connector.jdbc.source.JdbcSource;
import org.apache.flink.connector.jdbc.source.reader.extractor.ResultExtractor;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Timestamp;
import java.time.Instant;
import java.time.LocalDate;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.List;

/**
 * Sales Rankings -- Flink Batch Job (RustFS + DB --> DB)
 *
 * Sources:
 *   1. RustFS (S3-compatible)  sales_YYYYMMDD.csv files  ← one file per day in [from, to]
 *   2. PostgreSQL              source_sales table        ← date-filtered JDBC query
 *
 * Streams:
 *   A. Total Sales per City    (keyBy city       --> reduce)
 *   B. Total Sales per Salesman (keyBy salesmanId --> reduce)
 *
 * Sink:
 *   PostgreSQL  sales_ranks table  (upsert on conflict)
 *
 * Configuration via environment variables (set in docker-compose.yml):
 *   RUSTFS_BUCKET,
 *   SOURCE_DB_URL, SOURCE_DB_USER, SOURCE_DB_PASS,
 *   SINK_DB_URL,   SINK_DB_USER,   SINK_DB_PASS
 *
 * Required CLI arguments:
 *   --from  yyyy-MM-dd   inclusive lower bound — selects RustFS files and DB rows
 *   --to    yyyy-MM-dd   inclusive upper bound — selects RustFS files and DB rows
 *   e.g.  flink run ... flink-job.jar --from 2024-02-01 --to 2024-02-29
 */
public class BatchJob {

    private static final Logger log = LoggerFactory.getLogger(BatchJob.class);

    public static void main(String[] args) throws Exception {

        // Config from environment
        String rustfsBucket = env("RUSTFS_BUCKET",  "sales-csv");
        String sourceDbUrl  = env("SOURCE_DB_URL",  "jdbc:postgresql://postgres:5432/salesdb");
        String sourceDbUser = env("SOURCE_DB_USER", "poc");
        String sourceDbPass = env("SOURCE_DB_PASS", "poc123");
        String sinkDbUrl    = env("SINK_DB_URL",    "jdbc:postgresql://postgres:5432/salesdb");
        String sinkDbUser   = env("SINK_DB_USER",   "poc");
        String sinkDbPass   = env("SINK_DB_PASS",   "poc123");

        // Parse --from / --to CLI arguments (yyyy-MM-dd or epoch ms) — both required
        String fromStr = null;
        String toStr   = null;
        for (int i = 0; i < args.length - 1; i++) {
            if ("--from".equals(args[i])) fromStr = args[i + 1];
            if ("--to".equals(args[i]))   toStr   = args[i + 1];
        }
        if (fromStr == null || toStr == null) {
            throw new IllegalArgumentException("--from and --to are required. " +
                "Usage: flink run ... BatchJob --from yyyy-MM-dd --to yyyy-MM-dd");
        }

        long      fromEpochMs = parseDate(fromStr);
        long      toEpochMs   = parseDate(toStr);
        LocalDate fromDate    = parseLocalDate(fromStr);
        LocalDate toDate      = parseLocalDate(toStr);

        log.info("[BatchJob] Date range: {} to {} (epochMs: {} - {})", fromStr, toStr, fromEpochMs, toEpochMs);

        // SOURCE 1 -- RustFS (FileSource, one sales_YYYYMMDD.csv per day in [from, to])
        DateTimeFormatter dateFmt = DateTimeFormatter.ofPattern("yyyyMMdd");
        List<Path> rustfsPaths = new ArrayList<>();
        for (LocalDate d = fromDate; !d.isAfter(toDate); d = d.plusDays(1)) {
            rustfsPaths.add(new Path("s3a://" + rustfsBucket + "/sales_" + d.format(dateFmt) + ".csv"));
        }
        log.info("[BatchJob] Loading {} CSV file(s) from RustFS bucket '{}'", rustfsPaths.size(), rustfsBucket);

        FileSource<SaleEvent> rustfsSource = FileSource
            .forRecordStreamFormat(new CsvSaleEventFormat(), rustfsPaths.toArray(new Path[0]))
            .build(); // no monitorContinuously → bounded

        // SOURCE 2 -- JDBC (bounded query, date-filtered)
        String sql =
            "SELECT sale_id, salesman_id, salesman_name, city, region, " +
            "       product_id, amount, event_time " +
            "FROM   source_sales " +
            "WHERE  event_time >= " + fromEpochMs + " AND event_time <= " + toEpochMs + " " +
            "ORDER  BY event_time ASC";

        ResultExtractor<SaleEvent> extractor = rs -> {
            SaleEvent e = new SaleEvent();
            e.saleId       = rs.getString("sale_id");
            e.salesmanId   = rs.getString("salesman_id");
            e.salesmanName = rs.getString("salesman_name");
            e.city         = rs.getString("city");
            e.region       = rs.getString("region");
            e.productId    = rs.getString("product_id");
            e.amount       = rs.getDouble("amount");
            e.eventTime    = rs.getLong("event_time");
            e.source       = "db";
            return e;
        };

        JdbcSource<SaleEvent> jdbcSource = JdbcSource.<SaleEvent>builder()
            .setDriverName("org.postgresql.Driver")
            .setDBUrl(sourceDbUrl)
            .setUsername(sourceDbUser)
            .setPassword(sourceDbPass)
            .setSql(sql)
            .setResultExtractor(extractor)
            .setTypeInformation(TypeInformation.of(SaleEvent.class))
            .build();

        // BATCH mode: keyBy + reduce gives group-by semantics on bounded (finite) input,
        // emitting one final result per key rather than intermediate running totals.
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setRuntimeMode(RuntimeExecutionMode.BATCH);

        DataStream<SaleEvent> fromRustFS = env
            .fromSource(rustfsSource, WatermarkStrategy.noWatermarks(), "Source: RustFS/CSV");

        DataStream<SaleEvent> fromDb = env
            .fromSource(jdbcSource, WatermarkStrategy.noWatermarks(), "Source: JDBC/DB");

        // UNION -- merge both sources into one bounded stream
        DataStream<SaleEvent> allSales = fromRustFS.union(fromDb);

        // STREAM A -- Total Sales per City
        // keyBy city --> reduce --> map to SalesRank(CITY)
        // window_start = min event_time for that city, window_end = max event_time
        DataStream<SalesRank> topSalesPerCity = allSales
            .map(e -> new CityAcc(e.city, e.amount, e.eventTime, e.eventTime))
            .returns(TypeInformation.of(CityAcc.class)) // to enforce type info
            .keyBy(a -> a.city)
            .reduce((a, b) -> new CityAcc(
                a.city,
                a.total + b.total,
                Math.min(a.minTime, b.minTime),
                Math.max(a.maxTime, b.maxTime)))
            .map(a -> {
                SalesRank r = new SalesRank();
                r.rankType    = "CITY";
                r.groupKey    = a.city;
                r.entityId    = null;
                r.totalSales  = a.total;
                r.windowStart = new Timestamp(a.minTime);
                r.windowEnd   = new Timestamp(a.maxTime);
                return r;
            })
            .returns(TypeInformation.of(SalesRank.class)) // to enforce type info
            .name("Stream A: Total Sales per City");

        // STREAM B -- Total Sales per Salesman (country-wide)
        // keyBy salesmanId --> reduce --> map to SalesRank(COUNTRY)
        DataStream<SalesRank> topSalesmanCountry = allSales
            .map(e -> new SalesmanAcc(e.salesmanId, e.salesmanName, e.amount, e.eventTime, e.eventTime))
            .returns(TypeInformation.of(SalesmanAcc.class)) // to enforce type info
            .keyBy(a -> a.salesmanId)
            .reduce((a, b) -> new SalesmanAcc(
                a.salesmanId,
                a.salesmanName != null ? a.salesmanName : b.salesmanName,
                a.total + b.total,
                Math.min(a.minTime, b.minTime),
                Math.max(a.maxTime, b.maxTime)))
            .map(a -> {
                SalesRank r = new SalesRank();
                r.rankType    = "COUNTRY";
                r.groupKey    = a.salesmanName;
                r.entityId    = a.salesmanId;
                r.totalSales  = a.total;
                r.windowStart = new Timestamp(a.minTime);
                r.windowEnd   = new Timestamp(a.maxTime);
                return r;
            })
            .returns(TypeInformation.of(SalesRank.class)) // to enforce type info
            .name("Stream B: Total Sales per Salesman");

        // SINK -- PostgreSQL (both streams --> same table, different rank_type)
        JdbcConnectionOptions jdbcConnOpts = new JdbcConnectionOptions
            .JdbcConnectionOptionsBuilder()
            .withUrl(sinkDbUrl)
            .withDriverName("org.postgresql.Driver")
            .withUsername(sinkDbUser)
            .withPassword(sinkDbPass)
            .build();

        JdbcExecutionOptions jdbcExecOpts = JdbcExecutionOptions.builder()
            .withBatchSize(50)
            .withBatchIntervalMs(0)  // flush immediately in batch mode
            .withMaxRetries(3)
            .build();

        String upsertSql =
            "INSERT INTO sales_ranks (rank_type, group_key, entity_id, total_sales, window_start, window_end) " +
            "VALUES (?, ?, ?, ?, ?, ?) " +
            "ON CONFLICT (rank_type, group_key, window_end) " +
            "DO UPDATE SET total_sales = EXCLUDED.total_sales";

        topSalesPerCity.addSink(
            JdbcSink.sink(
                upsertSql,
                (stmt, r) -> {
                    stmt.setString(1, r.rankType);
                    stmt.setString(2, r.groupKey);
                    stmt.setString(3, r.entityId);   // null for CITY
                    stmt.setDouble(4, r.totalSales);
                    stmt.setTimestamp(5, r.windowStart);
                    stmt.setTimestamp(6, r.windowEnd);
                },
                jdbcExecOpts,
                jdbcConnOpts
            )
        ).name("Sink: City Totals --> PostgreSQL");

        topSalesmanCountry.addSink(
            JdbcSink.sink(
                upsertSql,
                (stmt, r) -> {
                    stmt.setString(1, r.rankType);
                    stmt.setString(2, r.groupKey);
                    stmt.setString(3, r.entityId);
                    stmt.setDouble(4, r.totalSales);
                    stmt.setTimestamp(5, r.windowStart);
                    stmt.setTimestamp(6, r.windowEnd);
                },
                jdbcExecOpts,
                jdbcConnOpts
            )
        ).name("Sink: Salesman Totals --> PostgreSQL");

        env.execute("Sales Rankings Batch Job");
    }

    private static long parseDate(String s) {
        try {
            return Long.parseLong(s);
        } catch (NumberFormatException ex) {
            return LocalDate.parse(s).atStartOfDay(ZoneOffset.UTC).toInstant().toEpochMilli();
        }
    }

    private static LocalDate parseLocalDate(String s) {
        try {
            long epochMs = Long.parseLong(s);
            return Instant.ofEpochMilli(epochMs).atZone(ZoneOffset.UTC).toLocalDate();
        } catch (NumberFormatException ex) {
            return LocalDate.parse(s);
        }
    }

    private static int indexOf(String[] arr, String val) {
        for (int i = 0; i < arr.length; i++) if (val.equals(arr[i])) return i;
        return -1;
    }

    private static String env(String key, String defaultValue) {
        String val = System.getenv(key);
        return (val != null && !val.isEmpty()) ? val : defaultValue;
    }

    // -- Accumulators (static inner POJOs for keyBy+reduce)

    public static class CityAcc {
        public String city;
        public double total;
        public long   minTime;
        public long   maxTime;

        public CityAcc() {}

        public CityAcc(String city, double total, long minTime, long maxTime) {
            this.city    = city;
            this.total   = total;
            this.minTime = minTime;
            this.maxTime = maxTime;
        }
    }

    public static class SalesmanAcc {
        public String salesmanId;
        public String salesmanName;
        public double total;
        public long   minTime;
        public long   maxTime;

        public SalesmanAcc() {}

        public SalesmanAcc(String id, String name, double total, long minTime, long maxTime) {
            this.salesmanId   = id;
            this.salesmanName = name;
            this.total        = total;
            this.minTime      = minTime;
            this.maxTime      = maxTime;
        }
    }
}
