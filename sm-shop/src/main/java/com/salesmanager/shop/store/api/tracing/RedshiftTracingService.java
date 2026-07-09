package com.salesmanager.shop.store.api.tracing;

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;
import java.sql.SQLException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.regex.Pattern;

@Service
public class RedshiftTracingService {

    private static final Logger LOGGER = LoggerFactory.getLogger(RedshiftTracingService.class);

    private static final Pattern IDENTIFIER_PATTERN = Pattern.compile("[A-Za-z_][A-Za-z0-9_]*");

    @Value("${redshift.jdbc.url:}")
    private String jdbcUrl;

    @Value("${redshift.user:}")
    private String user;

    @Value("${redshift.password:}")
    private String password;

    // @Value("${redshift.schema:public}")
    @Value("${redshift.schema:performance_tracking}")
    private String schema;

    @Value("${redshift.table:api_performance}")
    private String table;

    @Value("${redshift.tracing.datasource.driver-class-name:com.amazon.redshift.jdbc.Driver}")
    private String driverClassName;

    @Value("${redshift.pool.size:2}")
    private int poolSize;

    @Value("${redshift.connection.timeout.ms:30000}")
    private long connectionTimeoutMs;

    @Value("${redshift.startup.max.wait.ms:60000}")
    private long startupMaxWaitMs;

    @Value("${redshift.startup.retry.delay.ms:1000}")
    private long startupRetryDelayMs;

    @Value("${redshift.startup.check.query:select 1}")
    private String startupCheckQuery;

    @Value("${redshift.startup.check.timeout.seconds:15}")
    private int startupCheckTimeoutSeconds;

    @Value("${redshift.driver.connect.timeout.seconds:5}")
    private int driverConnectTimeoutSeconds;

    @Value("${redshift.driver.login.timeout.seconds:15}")
    private int driverLoginTimeoutSeconds;

    @Value("${redshift.driver.socket.timeout.seconds:30}")
    private int driverSocketTimeoutSeconds;

    @Value("${redshift.driver.tcp.keepalive:true}")
    private boolean driverTcpKeepAlive;

    @Value("${redshift.executor.threads:1}")
    private int executorThreads;

    @Value("${redshift.batch.size:100}")
    private int batchSize;

    @Value("${redshift.multirow.insert.rows:500}")
    private int multiRowInsertRows;

    @Value("${redshift.commit.every.flushes:1}")
    private int commitEveryFlushes;

    @Value("${redshift.flush.interval.ms:250}")
    private long flushIntervalMs;

    @Value("${redshift.queue.capacity:20000}")
    private int queueCapacity;

    private BlockingQueue<ApiTraceEvent> queue;
    private ExecutorService workers;
    private HikariDataSource dataSource;
    private String insertSqlPrefix;
    private volatile boolean running;
    private final AtomicLong droppedEvents = new AtomicLong();
    private final AtomicLong enqueuedEvents = new AtomicLong();
    private final AtomicLong insertedEvents = new AtomicLong();
    private final AtomicLong flushedBatches = new AtomicLong();
    private final AtomicLong failedBatches = new AtomicLong();
    private final AtomicBoolean unavailableLogged = new AtomicBoolean();

    private final TracingDestinationConfig tracingDestinationConfig;

    public RedshiftTracingService(TracingDestinationConfig tracingDestinationConfig) {
        this.tracingDestinationConfig = tracingDestinationConfig;
    }

    @PostConstruct
    public void init() {
        if (!tracingDestinationConfig.isApiEventDestinationEnabled("redshift")) {
            LOGGER.info("Redshift API tracing is disabled");
            return;
        }

        try {
            validateIdentifier("redshift.schema", schema);
            validateIdentifier("redshift.table", table);
            if (poolSize < executorThreads) {
                LOGGER.warn(
                        "Redshift pool size {} is smaller than executor thread count {}. This can cause internal sink contention.",
                        poolSize,
                        executorThreads
                );
            }
            LOGGER.info(
                    "Initializing Redshift tracing for {}.{} via {} (poolSize={}, workers={}, batchSize={}, statementRows={}, commitEveryFlushes={}, flushIntervalMs={}, queueCapacity={})",
                    schema,
                    table,
                    maskJdbcUrl(jdbcUrl),
                    poolSize,
                    executorThreads,
                    batchSize,
                    multiRowInsertRows,
                    commitEveryFlushes,
                    flushIntervalMs,
                    queueCapacity
            );

            HikariConfig config = new HikariConfig();
            config.setPoolName("redshift-tracing");
            config.setJdbcUrl(jdbcUrl);
            config.setUsername(user);
            config.setPassword(password);
            config.setDriverClassName(driverClassName);
            config.setMaximumPoolSize(poolSize);
            config.setMinimumIdle(Math.min(1, poolSize));
            config.setConnectionTimeout(connectionTimeoutMs);
            config.setAutoCommit(false);
            config.setInitializationFailTimeout(-1);
            config.addDataSourceProperty("connectTimeout", Integer.toString(Math.max(1, driverConnectTimeoutSeconds)));
            config.addDataSourceProperty("loginTimeout", Integer.toString(Math.max(1, driverLoginTimeoutSeconds)));
            config.addDataSourceProperty("socketTimeout", Integer.toString(Math.max(1, driverSocketTimeoutSeconds)));
            config.addDataSourceProperty("tcpKeepAlive", Boolean.toString(driverTcpKeepAlive));

            dataSource = new HikariDataSource(config);
            insertSqlPrefix = "insert into " + schema + "." + table
                    + " (event_id, request_id, event_timestamp, api_name, endpoint, status_code, error_type, user_id, client_id, session_id, payload)"
                    + " values ";
            verifyConnectivityWithRetry();

            queue = new ArrayBlockingQueue<>(queueCapacity);
            running = true;
            workers = Executors.newFixedThreadPool(executorThreads, new TraceWorkerFactory());
            for (int i = 0; i < executorThreads; i++) {
                workers.submit(this::flushLoop);
            }
            LOGGER.info("Redshift tracing initialized for {}.{} via {}", schema, table, jdbcUrl);
        } catch (Exception e) {
            LOGGER.error("Failed to initialize Redshift tracing — API tracing will be skipped", e);
            shutdownResources();
        }
    }

    public void traceAsync(ApiTraceEvent event) {
        if (queue == null) {
            if (unavailableLogged.compareAndSet(false, true)) {
                LOGGER.warn("Redshift tracing skipped because the sink is not initialized");
            }
            return;
        }
        if (!queue.offer(event)) {
            long dropped = droppedEvents.incrementAndGet();
            if (dropped == 1 || dropped % 100 == 0) {
                LOGGER.warn("Redshift trace queue is full, dropped {} event(s) so far", dropped);
            }
            return;
        }

        long queued = enqueuedEvents.incrementAndGet();
        if (queued == 1) {
            LOGGER.info(
                    "First Redshift trace event queued: eventId={}, endpoint={}, status={}",
                    event.getEventId(),
                    event.getEndpoint(),
                    event.getStatusCode()
            );
        }
    }

    private void flushLoop() {
        RedshiftSession session = null;
        try {
            while (running || (queue != null && !queue.isEmpty())) {
                List<ApiTraceEvent> batch = pollBatch();
                if (batch.isEmpty()) {
                    continue;
                }

                if (session == null) {
                    session = openSession();
                    if (session == null) {
                        requeueWithoutCounting(batch);
                        sleepQuietly(250);
                        continue;
                    }
                }

                try {
                    flushBatch(batch, session);
                } catch (SQLException e) {
                    failedBatches.incrementAndGet();
                    logSqlFailure(batch, batch.get(0), e);
                    rollbackSession(session);
                    closeSession(session, false);
                    session = null;
                } catch (Exception e) {
                    failedBatches.incrementAndGet();
                    LOGGER.error("Failed to insert Redshift trace batch of {} row(s)", batch.size(), e);
                    rollbackSession(session);
                    closeSession(session, false);
                    session = null;
                }
            }
        } finally {
            closeSession(session, true);
        }
    }

    private List<ApiTraceEvent> pollBatch() {
        List<ApiTraceEvent> batch = new ArrayList<>(batchSize);
        try {
            ApiTraceEvent first = queue.poll(flushIntervalMs, TimeUnit.MILLISECONDS);
            if (first == null) {
                return batch;
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
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            running = false;
        }
        return batch;
    }

    private RedshiftSession openSession() {
        if (dataSource == null) {
            return null;
        }
        try {
            Connection connection = dataSource.getConnection();
            return new RedshiftSession(connection);
        } catch (SQLException e) {
            LOGGER.error(
                    "Failed to open Redshift tracing session via {}. SQLState={}, vendorCode={}, message={}",
                    maskJdbcUrl(jdbcUrl),
                    e.getSQLState(),
                    e.getErrorCode(),
                    e.getMessage(),
                    e
            );
            return null;
        }
    }

    private void flushBatch(List<ApiTraceEvent> batch, RedshiftSession session) throws SQLException {
        ApiTraceEvent first = batch.get(0);
        long batchNumber = flushedBatches.incrementAndGet();
        int offset = 0;
        while (offset < batch.size()) {
            int rowsInStatement = Math.min(Math.max(1, multiRowInsertRows), batch.size() - offset);
            PreparedStatement statement = session.statementFor(rowsInStatement, buildInsertSql(rowsInStatement));
            int parameterIndex = 1;
            for (int i = 0; i < rowsInStatement; i++) {
                parameterIndex = bind(statement, batch.get(offset + i), parameterIndex);
            }
            statement.executeUpdate();
            statement.clearParameters();
            offset += rowsInStatement;
        }

        session.pendingRows += batch.size();
        session.pendingFlushes++;

        boolean committed = false;
        long totalInserted = insertedEvents.get();
        if (session.pendingFlushes >= Math.max(1, commitEveryFlushes)) {
            session.connection.commit();
            totalInserted = insertedEvents.addAndGet(session.pendingRows);
            session.pendingRows = 0;
            session.pendingFlushes = 0;
            committed = true;
        }

        if (committed && (batchNumber == 1 || batchNumber % 50 == 0)) {
            LOGGER.info(
                    "Committed Redshift trace batch #{} with {} row(s); totalInserted={}, queueDepth={}, firstEventId={}, firstEndpoint={}",
                    batchNumber,
                    batch.size(),
                    totalInserted,
                    queue != null ? queue.size() : 0,
                    first.getEventId(),
                    first.getEndpoint()
            );
        } else if (committed) {
            LOGGER.debug(
                    "Committed Redshift trace batch #{} with {} row(s); totalInserted={}, queueDepth={}",
                    batchNumber,
                    batch.size(),
                    totalInserted,
                    queue != null ? queue.size() : 0
            );
        } else if (LOGGER.isDebugEnabled()) {
            LOGGER.debug(
                    "Buffered Redshift trace batch #{} with {} row(s); pendingCommitRows={}, pendingFlushes={}, queueDepth={}",
                    batchNumber,
                    batch.size(),
                    session.pendingRows,
                    session.pendingFlushes,
                    queue != null ? queue.size() : 0
            );
        }
    }

    private int bind(PreparedStatement statement, ApiTraceEvent event, int index) throws SQLException {
        statement.setString(index++, event.getEventId());
        statement.setString(index++, event.getRequestId());
        statement.setTimestamp(index++, Timestamp.from(event.getEventTimestamp()));
        statement.setString(index++, event.getApiName());
        statement.setString(index++, event.getEndpoint());
        statement.setString(index++, event.getStatusCode());
        statement.setString(index++, defaultString(event.getErrorType()));
        statement.setString(index++, defaultString(event.getUserId()));
        statement.setString(index++, defaultString(event.getClientId()));
        statement.setString(index++, defaultString(event.getSessionId()));
        statement.setString(index++, defaultString(event.getPayload()));
        return index;
    }

    private String defaultString(String value) {
        return value != null ? value : "";
    }

    private String buildInsertSql(int rows) {
        StringBuilder sql = new StringBuilder(insertSqlPrefix.length() + rows * 40);
        sql.append(insertSqlPrefix);
        for (int i = 0; i < rows; i++) {
            if (i > 0) {
                sql.append(", ");
            }
            sql.append("(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)");
        }
        return sql.toString();
    }

    private void verifyConnectivityWithRetry() throws SQLException {
        long maxWaitMs = Math.max(connectionTimeoutMs, startupMaxWaitMs);
        long retryDelayMs = Math.max(250L, startupRetryDelayMs);
        long deadlineNanos = System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(maxWaitMs);
        long startedAtNanos = System.nanoTime();
        int attempts = 0;
        SQLException lastException = null;

        while (System.nanoTime() < deadlineNanos) {
            attempts++;
            try {
                verifyConnectivityOnce();
                if (attempts > 1) {
                    LOGGER.info(
                            "Redshift connectivity became ready after {} attempt(s) over {} ms",
                            attempts,
                            TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - startedAtNanos)
                    );
                }
                return;
            } catch (SQLException e) {
                lastException = e;
                long remainingMs = TimeUnit.NANOSECONDS.toMillis(deadlineNanos - System.nanoTime());
                if (remainingMs <= 0) {
                    break;
                }
                LOGGER.warn(
                        "Redshift connectivity check attempt {} failed (remaining startup wait ~{} ms). SQLState={}, vendorCode={}, message={}",
                        attempts,
                        remainingMs,
                        e.getSQLState(),
                        e.getErrorCode(),
                        e.getMessage()
                );
                sleepQuietly(Math.min(retryDelayMs, remainingMs));
                if (!running && Thread.currentThread().isInterrupted()) {
                    break;
                }
            }
        }

        if (lastException != null) {
            throw lastException;
        }
        throw new SQLException("Redshift connectivity check failed before startup timeout elapsed");
    }

    private void verifyConnectivityOnce() throws SQLException {
        try (Connection connection = dataSource.getConnection();
             PreparedStatement statement = connection.prepareStatement(startupCheckQuery)) {
            statement.setQueryTimeout(Math.max(1, startupCheckTimeoutSeconds));
            try (ResultSet resultSet = statement.executeQuery()) {
                if (resultSet.next()) {
                    LOGGER.info(
                            "Redshift connectivity check passed using query '{}' with firstValue={}",
                            startupCheckQuery,
                            abbreviate(resultSet.getString(1), 120)
                    );
                } else {
                    LOGGER.info("Redshift connectivity check passed using query '{}'", startupCheckQuery);
                }
            }
        }
    }

    private void logSqlFailure(List<ApiTraceEvent> batch, ApiTraceEvent first, SQLException e) {
        LOGGER.error(
                "Failed to insert Redshift trace batch of {} row(s) into {}.{} via {}. SQLState={}, vendorCode={}, message={}, firstEventId={}, firstEndpoint={}",
                batch.size(),
                schema,
                table,
                maskJdbcUrl(jdbcUrl),
                e.getSQLState(),
                e.getErrorCode(),
                e.getMessage(),
                first.getEventId(),
                first.getEndpoint(),
                e
        );
        SQLException next = e.getNextException();
        if (next != null) {
            LOGGER.error(
                    "Next Redshift SQL exception: SQLState={}, vendorCode={}, message={}",
                    next.getSQLState(),
                    next.getErrorCode(),
                    next.getMessage()
            );
        }
    }

    private void requeueWithoutCounting(List<ApiTraceEvent> batch) {
        if (queue == null || batch.isEmpty()) {
            return;
        }
        int requeued = 0;
        for (ApiTraceEvent event : batch) {
            if (queue.offer(event)) {
                requeued++;
            } else {
                long dropped = droppedEvents.incrementAndGet();
                if (dropped == 1 || dropped % 100 == 0) {
                    LOGGER.warn("Redshift trace queue is full, dropped {} event(s) so far", dropped);
                }
            }
        }
        if (requeued > 0 && LOGGER.isDebugEnabled()) {
            LOGGER.debug("Requeued {} Redshift trace event(s) after a transient session-open failure", requeued);
        }
    }

    private void sleepQuietly(long millis) {
        try {
            Thread.sleep(millis);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            running = false;
        }
    }

    private void validateIdentifier(String propertyName, String identifier) {
        if (identifier == null || !IDENTIFIER_PATTERN.matcher(identifier).matches()) {
            throw new IllegalArgumentException("Invalid " + propertyName + " value '" + identifier + "'");
        }
    }

    private String maskJdbcUrl(String url) {
        if (url == null || url.isEmpty()) {
            return "<empty>";
        }
        int scheme = url.indexOf("://");
        if (scheme < 0) {
            return url;
        }
        int hostStart = scheme + 3;
        int slash = url.indexOf('/', hostStart);
        if (slash < 0) {
            return url;
        }
        return url.substring(0, hostStart) + "***" + url.substring(slash);
    }

    private String abbreviate(String value, int max) {
        if (value == null || value.length() <= max) {
            return value;
        }
        return value.substring(0, max - 3) + "...";
    }

    @PreDestroy
    public void shutdown() {
        running = false;
        if (workers != null) {
            workers.shutdown();
            try {
                if (!workers.awaitTermination(10, TimeUnit.SECONDS)) {
                    workers.shutdownNow();
                }
            } catch (InterruptedException e) {
                workers.shutdownNow();
                Thread.currentThread().interrupt();
            }
        }
        LOGGER.info(
                "Shutting down Redshift tracing: enqueued={}, inserted={}, flushedBatches={}, failedBatches={}, dropped={}",
                enqueuedEvents.get(),
                insertedEvents.get(),
                flushedBatches.get(),
                failedBatches.get(),
                droppedEvents.get()
        );
        shutdownResources();
    }

    private void shutdownResources() {
        if (dataSource != null) {
            dataSource.close();
            dataSource = null;
        }
        queue = null;
    }

    private void rollbackSession(RedshiftSession session) {
        if (session == null) {
            return;
        }
        try {
            session.connection.rollback();
        } catch (Exception e) {
            LOGGER.debug("Error rolling back Redshift tracing transaction", e);
        } finally {
            session.pendingRows = 0;
            session.pendingFlushes = 0;
        }
    }

    private void closeSession(RedshiftSession session, boolean commitPending) {
        if (session == null) {
            return;
        }
        if (commitPending && session.pendingRows > 0) {
            try {
                session.connection.commit();
                long totalInserted = insertedEvents.addAndGet(session.pendingRows);
                LOGGER.info(
                        "Committed final Redshift trace transaction with {} buffered row(s); totalInserted={}",
                        session.pendingRows,
                        totalInserted
                );
            } catch (Exception e) {
                LOGGER.warn("Failed to commit buffered Redshift trace rows during session close", e);
                rollbackSession(session);
            } finally {
                session.pendingRows = 0;
                session.pendingFlushes = 0;
            }
        }
        for (PreparedStatement statement : session.statements.values()) {
            try {
                statement.close();
            } catch (Exception e) {
                LOGGER.debug("Error closing Redshift tracing statement", e);
            }
        }
        try {
            session.connection.close();
        } catch (Exception e) {
            LOGGER.debug("Error closing Redshift tracing connection", e);
        }
    }

    private static final class TraceWorkerFactory implements ThreadFactory {
        private final AtomicLong counter = new AtomicLong();

        @Override
        public Thread newThread(Runnable r) {
            Thread thread = new Thread(r, "redshift-trace-flusher-" + counter.incrementAndGet());
            thread.setDaemon(true);
            return thread;
        }
    }

    private static final class RedshiftSession {
        private final Connection connection;
        private final Map<Integer, PreparedStatement> statements = new HashMap<>();
        private int pendingFlushes;
        private long pendingRows;

        private RedshiftSession(Connection connection) {
            this.connection = connection;
        }

        private PreparedStatement statementFor(int rows, String sql) throws SQLException {
            PreparedStatement statement = statements.get(rows);
            if (statement == null || statement.isClosed()) {
                statement = connection.prepareStatement(sql);
                statements.put(rows, statement);
            }
            return statement;
        }
    }
}
