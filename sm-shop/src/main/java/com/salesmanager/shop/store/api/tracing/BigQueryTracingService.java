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
import java.util.ArrayList;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.List;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

@Service
public class BigQueryTracingService {

    private static final Logger LOGGER = LoggerFactory.getLogger(BigQueryTracingService.class);

    private static final DateTimeFormatter BQ_TIMESTAMP_FORMAT =
            DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss").withZone(ZoneOffset.UTC);

    /**
     * BigQuery fully-qualified table ID in the form PROJECT_ID.DATASET_ID.TABLE_ID.
     * Replace the placeholder with the actual table ID before deploying.
     */
    @Value("${bigquery.table.id:your-gcp-project-id.performance_tracking.api_performance}")
    private String tableId;

    @Value("${bigquery.batch.size:100}")
    private int batchSize;

    @Value("${bigquery.flush.interval.ms:250}")
    private long flushIntervalMs;

    @Value("${bigquery.queue.capacity:20000}")
    private int queueCapacity;

    private BigQuery bigQuery;
    private TableId bqTableId;
    private BlockingQueue<ApiTraceEvent> queue;
    private ExecutorService flusher;
    private volatile boolean running;
    private final AtomicLong droppedEvents = new AtomicLong();

    private final TracingDestinationConfig tracingDestinationConfig;

    public BigQueryTracingService(TracingDestinationConfig tracingDestinationConfig) {
        this.tracingDestinationConfig = tracingDestinationConfig;
    }

    @PostConstruct
    public void init() {
        if (!tracingDestinationConfig.isApiEventDestinationEnabled("bigquery")) {
            LOGGER.info("BigQuery API tracing is disabled");
            return;
        }
        try {
            bigQuery = BigQueryOptions.getDefaultInstance().getService();
            String[] parts = tableId.split("\\.");
            if (parts.length == 3) {
                bqTableId = TableId.of(parts[0], parts[1], parts[2]);
                queue = new ArrayBlockingQueue<>(queueCapacity);
                running = true;
                flusher = Executors.newSingleThreadExecutor(r -> {
                    Thread t = new Thread(r, "bigquery-trace-flusher");
                    t.setDaemon(true);
                    return t;
                });
                flusher.submit(this::flushLoop);
                LOGGER.info("BigQuery tracing initialized for table: {}", tableId);
            } else {
                LOGGER.warn("Invalid bigquery.table.id format '{}'. Expected PROJECT_ID.DATASET_ID.TABLE_ID — tracing disabled", tableId);
            }
        } catch (Exception e) {
            LOGGER.error("Failed to initialize BigQuery client — API tracing will be skipped", e);
        }
    }

    public void traceAsync(ApiTraceEvent event) {
        if (queue == null) {
            return;
        }
        if (!queue.offer(event)) {
            long dropped = droppedEvents.incrementAndGet();
            if (dropped == 1 || dropped % 100 == 0) {
                LOGGER.warn("BigQuery trace queue is full, dropped {} event(s) so far", dropped);
            }
        }
    }

    private void flushLoop() {
        while (running || (queue != null && !queue.isEmpty())) {
            List<ApiTraceEvent> batch = new ArrayList<>(batchSize);
            try {
                ApiTraceEvent first = queue.poll(flushIntervalMs, TimeUnit.MILLISECONDS);
                if (first == null) {
                    continue;
                }
                batch.add(first);

                long deadlineNanos = System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(flushIntervalMs);
                while (batch.size() < batchSize) {
                    queue.drainTo(batch, batchSize - batch.size());
                    if (batch.size() >= batchSize) {
                        break;
                    }

                    long remainingNanos = deadlineNanos - System.nanoTime();
                    if (remainingNanos <= 0) {
                        break;
                    }

                    ApiTraceEvent next = queue.poll(remainingNanos, TimeUnit.NANOSECONDS);
                    if (next == null) {
                        break;
                    }
                    batch.add(next);
                }

                flushBatch(batch);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                running = false;
            } catch (Exception e) {
                LOGGER.error("Unexpected error in BigQuery batch flusher", e);
            }
        }
    }

    private void flushBatch(List<ApiTraceEvent> batch) {
        if (batch.isEmpty()) {
            return;
        }
        try {
            InsertAllRequest.Builder requestBuilder = InsertAllRequest.newBuilder(bqTableId);
            for (ApiTraceEvent event : batch) {
                requestBuilder.addRow(event.getEventId(), toRow(event));
            }

            InsertAllRequest insertRequest = requestBuilder.build();

            InsertAllResponse response = bigQuery.insertAll(insertRequest);
            if (response.hasErrors()) {
                LOGGER.warn("BigQuery insert errors for {} row(s): {}", batch.size(), response.getInsertErrors());
            }
        } catch (Exception e) {
            LOGGER.error("Failed to insert BigQuery trace batch of {} row(s)", batch.size(), e);
        }
    }

    private Map<String, Object> toRow(ApiTraceEvent event) {
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
        return row;
    }

    @PreDestroy
    public void shutdown() {
        running = false;
        if (flusher != null) {
            flusher.shutdown();
            try {
                if (!flusher.awaitTermination(10, TimeUnit.SECONDS)) {
                    flusher.shutdownNow();
                }
            } catch (InterruptedException e) {
                flusher.shutdownNow();
                Thread.currentThread().interrupt();
            }
        }
    }
}
