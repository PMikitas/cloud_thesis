package com.salesmanager.shop.store.api.tracing;

import com.google.cloud.bigquery.BigQuery;
import com.google.cloud.bigquery.BigQueryOptions;
import com.google.cloud.bigquery.InsertAllRequest;
import com.google.cloud.bigquery.InsertAllResponse;
import com.google.cloud.bigquery.TableId;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

@Service
public class BigQueryTracingService {

    private static final Logger LOGGER = LoggerFactory.getLogger(BigQueryTracingService.class);

    private static final DateTimeFormatter BQ_TIMESTAMP_FORMAT =
            DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss").withZone(ZoneOffset.UTC);

    /**
     * BigQuery fully-qualified table ID in the form PROJECT_ID.DATASET_ID.TABLE_ID.
     * Replace the placeholder with the actual table ID before deploying.
     */
    @Value("project-366b6665-cdae-4555-80d.performance_tracking.api_performance")
    private String tableId;

    @Value("${bigquery.tracing.enabled:true}")
    private boolean tracingEnabled;

    private BigQuery bigQuery;
    private TableId bqTableId;
    private final ExecutorService executor = Executors.newFixedThreadPool(4);

    @PostConstruct
    public void init() {
        if (!tracingEnabled) {
            LOGGER.info("BigQuery API tracing is disabled");
            return;
        }
        try {
            bigQuery = BigQueryOptions.getDefaultInstance().getService();
            String[] parts = tableId.split("\\.");
            if (parts.length == 3) {
                bqTableId = TableId.of(parts[0], parts[1], parts[2]);
                LOGGER.info("BigQuery tracing initialized for table: {}", tableId);
            } else {
                LOGGER.warn("Invalid bigquery.table.id format '{}'. Expected PROJECT_ID.DATASET_ID.TABLE_ID — tracing disabled", tableId);
            }
        } catch (Exception e) {
            LOGGER.error("Failed to initialize BigQuery client — API tracing will be skipped", e);
        }
    }

    public void traceAsync(ApiTraceEvent event) {
        if (bigQuery == null || bqTableId == null) {
            return;
        }
        executor.submit(() -> doInsert(event));
    }

    private void doInsert(ApiTraceEvent event) {
        try {
            Map<String, Object> row = new HashMap<>();
            row.put("event_id", event.getEventId());
            row.put("request_id", event.getRequestId());
            row.put("event_timestamp", BQ_TIMESTAMP_FORMAT.format(event.getEventTimestamp()));
            row.put("api_name", event.getApiName());
            row.put("endpoint", event.getEndpoint());
            row.put("status_code", event.getStatusCode());
            row.put("error_type", event.getErrorType() != null ? event.getErrorType() : "");
            row.put("user_id", event.getUserId() != null ? event.getUserId() : "");
            row.put("client_id", event.getClientId() != null ? event.getClientId() : "");
            row.put("session_id", event.getSessionId() != null ? event.getSessionId() : "");
            row.put("payload", event.getPayload() != null ? event.getPayload() : "");

            InsertAllRequest insertRequest = InsertAllRequest.newBuilder(bqTableId)
                    .addRow(event.getEventId(), row)
                    .build();

            InsertAllResponse response = bigQuery.insertAll(insertRequest);
            if (response.hasErrors()) {
                LOGGER.warn("BigQuery insert errors for event {}: {}", event.getEventId(), response.getInsertErrors());
            }
        } catch (Exception e) {
            LOGGER.error("Failed to insert API trace event {} to BigQuery", event.getEventId(), e);
        }
    }

    @PreDestroy
    public void shutdown() {
        executor.shutdown();
        try {
            if (!executor.awaitTermination(5, TimeUnit.SECONDS)) {
                executor.shutdownNow();
            }
        } catch (InterruptedException e) {
            executor.shutdownNow();
            Thread.currentThread().interrupt();
        }
    }
}
