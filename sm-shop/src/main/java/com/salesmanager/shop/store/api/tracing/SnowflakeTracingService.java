package com.salesmanager.shop.store.api.tracing;

import net.snowflake.ingest.streaming.InsertValidationResponse;
import net.snowflake.ingest.streaming.OpenChannelRequest;
import net.snowflake.ingest.streaming.SnowflakeStreamingIngestChannel;
import net.snowflake.ingest.streaming.SnowflakeStreamingIngestClient;
import net.snowflake.ingest.streaming.SnowflakeStreamingIngestClientFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.time.LocalDateTime;

/**
 * Snowflake sink for {@link ApiTraceEvent} rows.
 *
 * Mirrors {@link BigQueryTracingService}: each event is submitted to an
 * executor and inserted as a single row via the SnowPipe Streaming SDK
 * (snowflake-ingest-sdk). The target table is expected to have the same
 * column layout as the BigQuery table:
 *   event_id STRING, request_id STRING, event_timestamp TIMESTAMP_NTZ,
 *   api_name STRING, endpoint STRING, status_code STRING, error_type STRING,
 *   user_id STRING, client_id STRING, session_id STRING, payload STRING
 *
 * All connection fields are externalised as {@code snowflake.*} properties
 * with placeholder defaults so the bean boots in any environment; replace
 * them with real values (or env-var overrides) before deploying.
 */
@Service
public class SnowflakeTracingService {

    private static final Logger LOGGER = LoggerFactory.getLogger(SnowflakeTracingService.class);

    private static final DateTimeFormatter SF_TIMESTAMP_FORMAT =
            DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss").withZone(ZoneOffset.UTC);

    @Value("${snowflake.tracing.enabled:true}")
    private boolean tracingEnabled;

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

    @Value("${snowflake.table:API_PERFORMANCE}")
    private String table;

    @Value("${snowflake.channel:shopizer_api_tracing_channel}")
    private String channelName;

    @Value("${snowflake.client.name:SHOPIZER_API_TRACER}")
    private String clientName;

    /**
     * Absolute path to a PEM file containing the PKCS#8 RSA private key
     * (e.g. /secrets/snowflake/ecomm_sf_key.p8). When set and readable, it
     * takes precedence over {@code snowflake.private.key}. The file may be
     * a full PEM (BEGIN/END markers) or just the base64 body.
     */
    @Value("${snowflake.private.key.path:/secrets/snowflake/ecomm_sf_key.p8}")
    private String privateKeyPath;

    /**
     * Inline PKCS#8 RSA private key (full PEM or just base64 body). Used
     * only when {@code snowflake.private.key.path} is empty. Avoid for
     * real credentials — properties files are awkward for multi-line PEMs.
     */
    @Value("${snowflake.private.key:PLACEHOLDER_PRIVATE_KEY}")
    private String privateKey;

    /** Optional passphrase for encrypted private keys; empty = none. */
    @Value("${snowflake.private.key.passphrase:}")
    private String privateKeyPassphrase;

    private SnowflakeStreamingIngestClient client;
    private volatile SnowflakeStreamingIngestChannel channel;
    private final ExecutorService executor = Executors.newFixedThreadPool(4);
    private final AtomicLong queuedEvents = new AtomicLong();
    private final AtomicLong insertedEvents = new AtomicLong();
    private final AtomicLong failedEvents = new AtomicLong();
    private final AtomicLong reopenedChannels = new AtomicLong();
    private final AtomicBoolean unavailableLogged = new AtomicBoolean();
    private final Object channelLock = new Object();

    @PostConstruct
    public void init() {
        if (!tracingEnabled) {
            LOGGER.info("Snowflake API tracing is disabled");
            return;
        }
        try {
            LOGGER.info(
                    "Initializing Snowflake tracing for {}.{}.{} on channel '{}' via {}",
                    database,
                    schema,
                    table,
                    channelName,
                    url
            );
            String resolvedKey = resolvePrivateKey();
            if (resolvedKey == null || resolvedKey.isEmpty()) {
                LOGGER.warn("Snowflake private key is empty — tracing disabled");
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
            LOGGER.info("Snowflake tracing initialized for {}.{}.{} on channel '{}'",
                    database, schema, table, channelName);
        } catch (Exception e) {
            LOGGER.error("Failed to initialize Snowflake streaming client — API tracing will be skipped", e);
        }
    }

    private OpenChannelRequest buildOpenChannelRequest() {
        return OpenChannelRequest.builder(channelName)
                .setDBName(database)
                .setSchemaName(schema)
                .setTableName(table)
                .setOnErrorOption(OpenChannelRequest.OnErrorOption.CONTINUE)
                .build();
    }

    /**
     * Loads the private key from {@code snowflake.private.key.path} when set,
     * otherwise returns the inline {@code snowflake.private.key} value. The
     * file may contain a full PEM or a raw base64 body; the SDK's
     * {@code Utils.parsePrivateKey} strips PEM markers and whitespace itself.
     *
     * Supports a leading {@code ~} as the current user's home directory.
     */
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

    public void traceAsync(ApiTraceEvent event) {
        if (channel == null) {
            if (unavailableLogged.compareAndSet(false, true)) {
                LOGGER.warn("Snowflake tracing skipped because the sink is not initialized");
            }
            return;
        }
        long queued = queuedEvents.incrementAndGet();
        if (queued == 1) {
            LOGGER.info(
                    "First Snowflake trace event queued: eventId={}, endpoint={}, status={}",
                    event.getEventId(),
                    event.getEndpoint(),
                    event.getStatusCode()
            );
        }
        executor.submit(() -> doInsert(event));
    }

    private void doInsert(ApiTraceEvent event) {
        SnowflakeStreamingIngestChannel attemptedChannel = channel;
        try {
            insertEvent(event, attemptedChannel);
        } catch (Exception e) {
            if (isInvalidChannelException(e) && reopenChannel(attemptedChannel, e.getMessage())) {
                try {
                    insertEvent(event, channel);
                    return;
                } catch (Exception retryException) {
                    failedEvents.incrementAndGet();
                    LOGGER.error(
                            "Failed to insert API trace event {} to Snowflake after reopening the channel",
                            event.getEventId(),
                            retryException
                    );
                    return;
                }
            }
            failedEvents.incrementAndGet();
            LOGGER.error("Failed to insert API trace event {} to Snowflake", event.getEventId(), e);
        }
    }

    private void insertEvent(ApiTraceEvent event, SnowflakeStreamingIngestChannel activeChannel) throws Exception {
        if (activeChannel == null) {
            throw new IllegalStateException("Snowflake streaming channel is not initialized");
        }

        Map<String, Object> row = new HashMap<>();
        row.put("event_id", event.getEventId());
        row.put("request_id", event.getRequestId());
        row.put("event_timestamp", LocalDateTime.ofInstant(event.getEventTimestamp(), ZoneOffset.UTC));
        row.put("api_name", event.getApiName());
        row.put("endpoint", event.getEndpoint());
        row.put("status_code", event.getStatusCode());
        row.put("error_type", event.getErrorType() != null ? event.getErrorType() : "");
        row.put("user_id", event.getUserId() != null ? event.getUserId() : "");
        row.put("client_id", event.getClientId() != null ? event.getClientId() : "");
        row.put("session_id", event.getSessionId() != null ? event.getSessionId() : "");
        row.put("payload", event.getPayload() != null ? event.getPayload() : "");

        InsertValidationResponse response = activeChannel.insertRow(row, event.getEventId());
        if (response.hasErrors()) {
            failedEvents.incrementAndGet();
            LOGGER.warn(
                    "Snowflake insert errors for event {}: {}",
                    event.getEventId(),
                    formatInsertErrors(response.getInsertErrors())
            );
            return;
        }

        long inserted = insertedEvents.incrementAndGet();
        if (inserted == 1 || inserted % 500 == 0) {
            LOGGER.info(
                    "Inserted Snowflake trace event count={} latestEventId={} endpoint={}",
                    inserted,
                    event.getEventId(),
                    event.getEndpoint()
            );
        } else if (LOGGER.isDebugEnabled()) {
            LOGGER.debug(
                    "Inserted Snowflake trace event latestEventId={} endpoint={}",
                    event.getEventId(),
                    event.getEndpoint()
            );
        }
    }

    private boolean reopenChannel(SnowflakeStreamingIngestChannel failedChannel, String reason) {
        synchronized (channelLock) {
            if (client == null) {
                LOGGER.warn("Cannot reopen Snowflake tracing channel because the client is not initialized");
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
                        LOGGER.warn("Error closing invalid Snowflake tracing channel before reopen", closeException);
                    }
                }
                channel = client.openChannel(buildOpenChannelRequest());
                unavailableLogged.set(false);
                long reopened = reopenedChannels.incrementAndGet();
                LOGGER.warn(
                        "Reopened Snowflake tracing channel '{}' for {}.{}.{} count={} reason={}",
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
                        "Failed to reopen Snowflake tracing channel '{}' for {}.{}.{}",
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
        try {
            if (channel != null) {
                channel.close().get(5, TimeUnit.SECONDS);
            }
        } catch (Exception e) {
            LOGGER.warn("Error closing Snowflake streaming channel", e);
        }
        try {
            if (client != null) {
                client.close();
            }
        } catch (Exception e) {
            LOGGER.warn("Error closing Snowflake streaming client", e);
        }
        LOGGER.info(
                "Shutting down Snowflake tracing: queued={}, inserted={}, failed={}, reopenedChannels={}",
                queuedEvents.get(),
                insertedEvents.get(),
                failedEvents.get(),
                reopenedChannels.get()
        );
    }
}
