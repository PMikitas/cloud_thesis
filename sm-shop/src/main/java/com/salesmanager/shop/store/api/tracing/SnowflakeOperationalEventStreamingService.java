package com.salesmanager.shop.store.api.tracing;

import com.salesmanager.core.model.operational.OperationalEventOutbox;
import net.snowflake.ingest.streaming.InsertValidationResponse;
import net.snowflake.ingest.streaming.OpenChannelRequest;
import net.snowflake.ingest.streaming.SnowflakeStreamingIngestChannel;
import net.snowflake.ingest.streaming.SnowflakeStreamingIngestClient;
import net.snowflake.ingest.streaming.SnowflakeStreamingIngestClientFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;
import org.springframework.transaction.support.TransactionTemplate;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;
import javax.persistence.EntityManager;
import javax.persistence.PersistenceContext;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

@Service
public class SnowflakeOperationalEventStreamingService {

    private static final Logger LOGGER = LoggerFactory.getLogger(SnowflakeOperationalEventStreamingService.class);
    private static final long INSERTED_LOG_INTERVAL = 1_000;
    private static final long FAILED_LOG_INTERVAL = 100;
    private static final int OUTBOX_UPDATE_CHUNK_SIZE = 5_000;

    @Value("${snowflake.operational.events.enabled:false}")
    private boolean enabled;

    @Value("${snowflake.url:https://your-account.region.cloud.snowflakecomputing.com}")
    private String url;

    @Value("${snowflake.user:PLACEHOLDER_USER}")
    private String user;

    @Value("${snowflake.role:PLACEHOLDER_ROLE}")
    private String role;

    @Value("${snowflake.warehouse:PLACEHOLDER_WAREHOUSE}")
    private String warehouse;

    @Value("${snowflake.database:PLACEHOLDER_DATABASE}")
    private String database;

    @Value("${snowflake.schema:PLACEHOLDER_SCHEMA}")
    private String schema;

    @Value("${snowflake.operational.events.table:OPERATIONAL_EVENTS}")
    private String table;

    @Value("${snowflake.operational.events.channel:shopizer_operational_events_channel}")
    private String channelName;

    @Value("${snowflake.operational.events.client.name:SHOPIZER_OPERATIONAL_EVENT_TRACER}")
    private String clientName;

    @Value("${snowflake.operational.events.batch.size:200}")
    private int batchSize;

    @Value("${snowflake.operational.events.max.batches.per.flush:1}")
    private int maxBatchesPerFlush;

    @Value("${snowflake.operational.events.max.flush.duration.ms:1000}")
    private long maxFlushDurationMs;

    @Value("${snowflake.operational.events.outbox.purge.enabled:true}")
    private boolean purgeSentOutboxEnabled;

    @Value("${snowflake.operational.events.outbox.purge.retention.ms:300000}")
    private long purgeSentOutboxRetentionMs;

    @Value("${snowflake.operational.events.outbox.purge.batch.size:10000}")
    private int purgeSentOutboxBatchSize;

    @Value("${snowflake.private.key.path:/secrets/snowflake/ecomm_sf_key.p8}")
    private String privateKeyPath;

    @Value("${snowflake.private.key:PLACEHOLDER_PRIVATE_KEY}")
    private String privateKey;

    @Value("${snowflake.private.key.passphrase:}")
    private String privateKeyPassphrase;

    @PersistenceContext
    private EntityManager entityManager;

    private final TransactionTemplate transactionTemplate;
    private SnowflakeStreamingIngestClient client;
    private volatile SnowflakeStreamingIngestChannel channel;
    private final Object channelLock = new Object();
    private final AtomicLong insertAttempts = new AtomicLong();
    private final AtomicLong insertedEvents = new AtomicLong();
    private final AtomicLong failedEvents = new AtomicLong();
    private final AtomicLong purgedOutboxRows = new AtomicLong();
    private final AtomicLong reopenedChannels = new AtomicLong();
    private final AtomicBoolean unavailableLogged = new AtomicBoolean();

    public SnowflakeOperationalEventStreamingService(TransactionTemplate transactionTemplate) {
        this.transactionTemplate = transactionTemplate;
    }

    @PostConstruct
    public void init() {
        if (!enabled) {
            LOGGER.info("Snowflake operational event streaming is disabled");
            return;
        }

        try {
            LOGGER.info(
                    "Initializing Snowflake operational event streaming for {}.{}.{} on channel '{}' via {}",
                    database,
                    schema,
                    table,
                    channelName,
                    url
            );
            String resolvedKey = resolvePrivateKey();
            if (resolvedKey == null || resolvedKey.isEmpty()) {
                LOGGER.warn("Snowflake private key is empty — operational event streaming disabled");
                return;
            }

            Properties props = new Properties();
            props.put("url", url);
            props.put("user", user);
            props.put("role", role);
            props.put("warehouse", warehouse);
            props.put("database", database);
            props.put("schema", schema);
            props.put("private_key", resolvedKey);
            if (privateKeyPassphrase != null && !privateKeyPassphrase.isEmpty()) {
                props.put("private_key_passphrase", privateKeyPassphrase);
            }
            props.put("scheme", "https");
            props.put("port", "443");

            client = SnowflakeStreamingIngestClientFactory.builder(clientName)
                    .setProperties(props)
                    .build();

            channel = client.openChannel(buildOpenChannelRequest());
            LOGGER.info("Snowflake operational event streaming initialized for {}.{}.{} on channel '{}'",
                    database, schema, table, channelName);
        } catch (Exception e) {
            LOGGER.error("Failed to initialize Snowflake operational event streaming — outbox rows will remain local", e);
        }
    }

    @Scheduled(fixedDelayString = "${snowflake.operational.events.flush.fixed.delay.ms:1000}")
    public void flushOutbox() {
        if (!enabled || channel == null) {
            logUnavailableOnce();
            return;
        }

        int effectiveBatchSize = Math.max(1, batchSize);
        int maxBatches = Math.max(1, maxBatchesPerFlush);
        long deadlineNanos = System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(Math.max(1L, maxFlushDurationMs));
        int batches = 0;
        int selectedRows = 0;
        int insertedRows = 0;

        while (batches < maxBatches && System.nanoTime() < deadlineNanos) {
            BatchResult result = transactionTemplate.execute(status -> flushNextBatch(effectiveBatchSize));
            if (result == null || result.selected == 0) {
                break;
            }

            batches++;
            selectedRows += result.selected;
            insertedRows += result.inserted;

            if (result.inserted < result.selected || result.selected < effectiveBatchSize) {
                break;
            }
        }

        if (batches > 1 && insertedRows > 0) {
            LOGGER.info(
                    "Snowflake operational event drain summary batches={} selected={} inserted={} attempts={} failed={} reachedBatchLimit={} reachedTimeLimit={}",
                    batches,
                    selectedRows,
                    insertedRows,
                    insertAttempts.get(),
                    failedEvents.get(),
                    batches >= maxBatches,
                    System.nanoTime() >= deadlineNanos
            );
        }
    }

    @Scheduled(fixedDelayString = "${snowflake.operational.events.outbox.purge.fixed.delay.ms:10000}")
    public void purgeSentOutboxRows() {
        if (!enabled || !purgeSentOutboxEnabled) {
            return;
        }

        try {
            int batchLimit = Math.max(1, purgeSentOutboxBatchSize);
            long retentionMs = Math.max(0L, purgeSentOutboxRetentionMs);
            Date cutoff = new Date(System.currentTimeMillis() - retentionMs);
            Integer purged = transactionTemplate.execute(status -> entityManager
                    .createNativeQuery(
                            "DELETE FROM OPERATIONAL_EVENT_OUTBOX " +
                                    "WHERE SENT_AT IS NOT NULL AND SENT_AT < ? " +
                                    "ORDER BY OUTBOX_ID LIMIT ?"
                    )
                    .setParameter(1, cutoff)
                    .setParameter(2, batchLimit)
                    .executeUpdate());
            if (purged != null && purged > 0) {
                long totalPurged = purgedOutboxRows.addAndGet(purged);
                LOGGER.info(
                        "Purged sent Snowflake operational outbox rows count={} totalPurged={} retentionMs={} batchLimit={}",
                        purged,
                        totalPurged,
                        retentionMs,
                        batchLimit
                );
            }
        } catch (Exception e) {
            LOGGER.warn("Could not purge sent Snowflake operational outbox rows", e);
        }
    }

    private BatchResult flushNextBatch(int effectiveBatchSize) {
        List<OperationalEventOutbox> batch = entityManager
                .createQuery(
                        "select e from OperationalEventOutbox e where e.sentAt is null order by e.id",
                        OperationalEventOutbox.class
                )
                .setMaxResults(effectiveBatchSize)
                .getResultList();
        if (batch.isEmpty()) {
            return BatchResult.empty();
        }

        bulkIncrementAttempts(batch);

        int inserted;
        SnowflakeStreamingIngestChannel attemptedChannel = channel;
        try {
            InsertValidationResponse response = insertBatch(batch, attemptedChannel);
            inserted = applyInsertResponse(batch, response);
        } catch (Exception e) {
            if (isInvalidChannelException(e) && reopenChannel(attemptedChannel, e.getMessage())) {
                try {
                    InsertValidationResponse response = insertBatch(batch, channel);
                    inserted = applyInsertResponse(batch, response);
                } catch (Exception retryException) {
                    markFailures(batch, retryException);
                    inserted = 0;
                }
            } else {
                markFailures(batch, e);
                inserted = 0;
            }
        }

        entityManager.flush();
        entityManager.clear();
        if (inserted > 0) {
            long totalInserted = insertedEvents.addAndGet(inserted);
            if (totalInserted == inserted || totalInserted % INSERTED_LOG_INTERVAL < inserted) {
                LOGGER.info(
                        "Snowflake operational event rows inserted count={} attempts={} failed={} latestBatchSize={}",
                        totalInserted,
                        insertAttempts.get(),
                        failedEvents.get(),
                        batch.size()
                );
            }
        }
        return new BatchResult(batch.size(), inserted);
    }

    private InsertValidationResponse insertBatch(
            List<OperationalEventOutbox> batch,
            SnowflakeStreamingIngestChannel activeChannel
    ) throws Exception {
        if (activeChannel == null) {
            throw new IllegalStateException("Snowflake operational event streaming channel is not initialized");
        }
        insertAttempts.addAndGet(batch.size());

        List<Map<String, Object>> rows = new ArrayList<>(batch.size());
        for (OperationalEventOutbox event : batch) {
            rows.add(buildRow(event));
        }

        String offsetToken = batch.get(batch.size() - 1).getEventId();
        return activeChannel.insertRows(rows, offsetToken);
    }

    private Map<String, Object> buildRow(OperationalEventOutbox event) {
        Map<String, Object> row = new HashMap<>();
        row.put("event_id", event.getEventId());
        row.put("outbox_id", event.getId());
        row.put("occurred_at", LocalDateTime.ofInstant(event.getOccurredAt().toInstant(), ZoneOffset.UTC));
        row.put("entity_name", event.getEntityName());
        row.put("table_name", event.getTableName());
        row.put("entity_id", event.getEntityId() != null ? event.getEntityId() : "");
        row.put("operation", event.getOperation());
        row.put("payload", event.getPayload());
        return row;
    }

    private int applyInsertResponse(List<OperationalEventOutbox> batch, InsertValidationResponse response) {
        if (!response.hasErrors()) {
            bulkMarkSent(batch);
            return batch.size();
        }

        String batchErrors = formatInsertErrors(response.getInsertErrors());
        Map<Long, String> errorsByRow = new HashMap<>();
        for (InsertValidationResponse.InsertError error : response.getInsertErrors()) {
            errorsByRow.put(error.getRowIndex(), batchErrors);
        }

        int inserted = 0;
        List<OperationalEventOutbox> successfulRows = new ArrayList<>(batch.size());
        List<OperationalEventOutbox> failedRows = new ArrayList<>();
        for (int i = 0; i < batch.size(); i++) {
            OperationalEventOutbox event = batch.get(i);
            String error = errorsByRow.get((long) i);
            if (error == null) {
                successfulRows.add(event);
                inserted++;
            } else {
                failedRows.add(event);
            }
        }
        bulkMarkSent(successfulRows);
        bulkMarkFailures(failedRows, new IllegalStateException(batchErrors));
        return inserted;
    }

    private void bulkIncrementAttempts(List<OperationalEventOutbox> batch) {
        List<Long> ids = idsOf(batch);
        if (ids.isEmpty()) {
            return;
        }
        for (int start = 0; start < ids.size(); start += OUTBOX_UPDATE_CHUNK_SIZE) {
            entityManager
                    .createQuery("update OperationalEventOutbox e set e.attemptCount = e.attemptCount + 1 where e.id in :ids")
                    .setParameter("ids", ids.subList(start, Math.min(start + OUTBOX_UPDATE_CHUNK_SIZE, ids.size())))
                    .executeUpdate();
        }
    }

    private void bulkMarkSent(List<OperationalEventOutbox> batch) {
        List<Long> ids = idsOf(batch);
        if (ids.isEmpty()) {
            return;
        }
        Date sentAt = new Date();
        for (int start = 0; start < ids.size(); start += OUTBOX_UPDATE_CHUNK_SIZE) {
            entityManager
                    .createQuery("update OperationalEventOutbox e set e.sentAt = :sentAt, e.lastError = null where e.id in :ids")
                    .setParameter("sentAt", sentAt)
                    .setParameter("ids", ids.subList(start, Math.min(start + OUTBOX_UPDATE_CHUNK_SIZE, ids.size())))
                    .executeUpdate();
        }
    }

    private void markFailures(List<OperationalEventOutbox> batch, Exception e) {
        bulkMarkFailures(batch, e);
    }

    private void bulkMarkFailures(List<OperationalEventOutbox> batch, Exception e) {
        List<Long> ids = idsOf(batch);
        if (ids.isEmpty()) {
            return;
        }
        for (OperationalEventOutbox event : batch) {
            logFailure(event, e);
        }
        String lastError = abbreviate(e.getMessage(), 4000);
        for (int start = 0; start < ids.size(); start += OUTBOX_UPDATE_CHUNK_SIZE) {
            entityManager
                    .createQuery("update OperationalEventOutbox e set e.lastError = :lastError where e.id in :ids")
                    .setParameter("lastError", lastError)
                    .setParameter("ids", ids.subList(start, Math.min(start + OUTBOX_UPDATE_CHUNK_SIZE, ids.size())))
                    .executeUpdate();
        }
    }

    private List<Long> idsOf(List<OperationalEventOutbox> batch) {
        List<Long> ids = new ArrayList<>(batch.size());
        for (OperationalEventOutbox event : batch) {
            ids.add(event.getId());
        }
        return ids;
    }

    private OpenChannelRequest buildOpenChannelRequest() {
        return OpenChannelRequest.builder(channelName)
                .setDBName(database)
                .setSchemaName(schema)
                .setTableName(table)
                .setOnErrorOption(OpenChannelRequest.OnErrorOption.CONTINUE)
                .build();
    }

    private void logFailure(OperationalEventOutbox event, Exception e) {
        long failed = failedEvents.incrementAndGet();
        boolean shouldLog = failed == 1 || failed % FAILED_LOG_INTERVAL == 0;
        if (shouldLog) {
            LOGGER.warn(
                    "Snowflake operational event insert unsuccessful count={} attempts={} outboxId={} table={} operation={} reason={}",
                    failed,
                    insertAttempts.get(),
                    event.getId(),
                    event.getTableName(),
                    event.getOperation(),
                    e.getMessage(),
                    e
            );
        } else {
            LOGGER.debug(
                    "Snowflake operational event insert unsuccessful outboxId={} table={} operation={} reason={}",
                    event.getId(),
                    event.getTableName(),
                    event.getOperation(),
                    e.getMessage()
            );
        }
    }

    private boolean reopenChannel(SnowflakeStreamingIngestChannel failedChannel, String reason) {
        synchronized (channelLock) {
            if (client == null) {
                LOGGER.warn("Cannot reopen Snowflake operational event channel because the client is not initialized");
                return false;
            }

            if (failedChannel != null && channel != failedChannel) {
                return true;
            }

            try {
                if (failedChannel != null) {
                    try {
                        failedChannel.close().get(5, TimeUnit.SECONDS);
                    } catch (Exception closeException) {
                        LOGGER.warn("Error closing invalid Snowflake operational event channel before reopen", closeException);
                    }
                }
                channel = client.openChannel(buildOpenChannelRequest());
                unavailableLogged.set(false);
                long reopened = reopenedChannels.incrementAndGet();
                LOGGER.warn(
                        "Reopened Snowflake operational event channel '{}' for {}.{}.{} count={} reason={}",
                        channelName,
                        database,
                        schema,
                        table,
                        reopened,
                        reason
                );
                return true;
            } catch (Exception reopenException) {
                LOGGER.error(
                        "Failed to reopen Snowflake operational event channel '{}' for {}.{}.{}",
                        channelName,
                        database,
                        schema,
                        table,
                        reopenException
                );
                channel = null;
                return false;
            }
        }
    }

    private boolean isInvalidChannelException(Throwable throwable) {
        Throwable current = throwable;
        while (current != null) {
            String message = current.getMessage();
            if (message != null
                    && message.toLowerCase().contains("channel")
                    && message.toLowerCase().contains("invalid")) {
                return true;
            }
            current = current.getCause();
        }
        return false;
    }

    private String resolvePrivateKey() {
        if (privateKeyPath != null && !privateKeyPath.trim().isEmpty()) {
            String expanded = privateKeyPath.trim();
            if (expanded.startsWith("~")) {
                expanded = System.getProperty("user.home") + expanded.substring(1);
            }
            try {
                Path p = Paths.get(expanded);
                String content = new String(Files.readAllBytes(p), StandardCharsets.UTF_8).trim();
                LOGGER.info("Loaded Snowflake private key from {}", p.toAbsolutePath());
                return content;
            } catch (Exception e) {
                LOGGER.error("Failed to read Snowflake private key from '{}'", expanded, e);
                return "";
            }
        }
        return privateKey;
    }

    private void logUnavailableOnce() {
        if (enabled && unavailableLogged.compareAndSet(false, true)) {
            LOGGER.warn("Snowflake operational event streaming is enabled but channel is not initialized");
        }
    }

    private String formatInsertErrors(List<InsertValidationResponse.InsertError> errors) {
        StringBuilder out = new StringBuilder();
        for (int i = 0; i < errors.size(); i++) {
            InsertValidationResponse.InsertError error = errors.get(i);
            if (i > 0) {
                out.append(" | ");
            }
            out.append("rowIndex=").append(error.getRowIndex());
            if (error.getMessage() != null && !error.getMessage().isEmpty()) {
                out.append(", message=").append(error.getMessage());
            }
            if (error.getMissingNotNullColNames() != null && !error.getMissingNotNullColNames().isEmpty()) {
                out.append(", missingNotNull=").append(error.getMissingNotNullColNames());
            }
            if (error.getExtraColNames() != null && !error.getExtraColNames().isEmpty()) {
                out.append(", extraCols=").append(error.getExtraColNames());
            }
            if (error.getException() != null) {
                out.append(", exception=").append(error.getException().getMessage());
            }
            if (error.getRowContent() != null) {
                out.append(", rowContent=").append(abbreviate(String.valueOf(error.getRowContent()), 300));
            }
        }
        return out.toString();
    }

    private String abbreviate(String value, int max) {
        if (value == null || value.length() <= max) {
            return value;
        }
        return value.substring(0, max - 3) + "...";
    }

    private static class BatchResult {
        private final int selected;
        private final int inserted;

        private BatchResult(int selected, int inserted) {
            this.selected = selected;
            this.inserted = inserted;
        }

        private static BatchResult empty() {
            return new BatchResult(0, 0);
        }
    }

    @PreDestroy
    public void shutdown() {
        try {
            if (channel != null) {
                channel.close().get(5, TimeUnit.SECONDS);
            }
        } catch (Exception e) {
            LOGGER.warn("Error closing Snowflake operational event channel", e);
        }
        try {
            if (client != null) {
                client.close();
            }
        } catch (Exception e) {
            LOGGER.warn("Error closing Snowflake operational event client", e);
        }
    }
}
