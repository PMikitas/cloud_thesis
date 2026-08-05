package com.salesmanager.shop.store.api.tracing;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.salesmanager.core.model.generic.SalesManagerEntity;
import com.salesmanager.core.model.operational.OperationalEventBridge;
import com.salesmanager.core.model.operational.OperationalEventOperation;
import com.salesmanager.core.model.operational.OperationalEventSink;
import org.hibernate.Hibernate;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Service;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;
import javax.persistence.Column;
import javax.persistence.Embedded;
import javax.persistence.JoinColumn;
import javax.persistence.ManyToOne;
import javax.persistence.OneToOne;
import javax.persistence.Table;
import javax.persistence.Transient;
import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.sql.Timestamp;
import java.time.Instant;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Date;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Collectors;

@Service
public class OperationalEntityOutboxService implements OperationalEventSink {

    private static final Logger LOGGER = LoggerFactory.getLogger(OperationalEntityOutboxService.class);
    private static final DateTimeFormatter UTC_FORMATTER = DateTimeFormatter.ISO_INSTANT;
    private static final int PAYLOAD_SCHEMA_VERSION = 1;
    private static final int MAX_EMBEDDED_DEPTH = 2;
    private static final Set<String> SENSITIVE_TOKENS = Collections.unmodifiableSet(new LinkedHashSet<>(Arrays.asList(
            "password",
            "passwd",
            "secret",
            "private",
            "credential",
            "token",
            "creditcard",
            "credit_card",
            "cardnumber",
            "card_number",
            "cvv",
            "cvc",
            "pan"
    )));

    @Value("${snowflake.operational.events.enabled:false}")
    private boolean enabled;

    @Value("${snowflake.operational.events.tables:CUSTOMER,PRODUCT,PRODUCT_AVAILABILITY,PRODUCT_DESCRIPTION,PRODUCT_PRICE,SHOPPING_CART,SHOPPING_CART_ITEM,ORDERS,ORDER_PRODUCT,ORDER_TOTAL,ORDER_STATUS_HISTORY,SM_TRANSACTION}")
    private String configuredTables;

    @Value("${db.schema:}")
    private String dbSchema;

    private final ObjectMapper objectMapper;
    private final JdbcTemplate jdbcTemplate;
    private volatile Set<String> trackedTables = Collections.emptySet();
    private volatile String outboxTableName = "OPERATIONAL_EVENT_OUTBOX";

    public OperationalEntityOutboxService(ObjectMapper objectMapper, JdbcTemplate jdbcTemplate) {
        this.objectMapper = objectMapper;
        this.jdbcTemplate = jdbcTemplate;
    }

    @PostConstruct
    public void init() {
        trackedTables = parseTrackedTables(configuredTables);
        outboxTableName = qualifyOutboxTableName(dbSchema);
        if (!enabled) {
            LOGGER.info("Snowflake operational event outbox is disabled");
            return;
        }
        OperationalEventBridge.register(this);
        LOGGER.info("Snowflake operational event outbox enabled for tables={}", trackedTables);
    }

    @PreDestroy
    public void shutdown() {
        OperationalEventBridge.unregister(this);
    }

    @Override
    public void capture(Object entity, OperationalEventOperation operation) {
        if (!enabled || entity == null || operation == null) {
            return;
        }

        try {
            Class<?> entityClass = Hibernate.getClass(entity);
            Table table = entityClass.getAnnotation(Table.class);
            if (table == null || table.name() == null || table.name().trim().isEmpty()) {
                return;
            }

            String tableName = table.name().trim().toUpperCase(Locale.ROOT);
            if (!trackedTables.contains("*") && !trackedTables.contains(tableName)) {
                return;
            }

            PendingOperationalEvent pending = buildPendingEvent(entity, entityClass, tableName, operation);
            insertOutboxEvent(pending);
        } catch (Exception e) {
            LOGGER.warn("Could not capture operational event for entity={} operation={}", entity.getClass().getName(), operation, e);
        }
    }

    private PendingOperationalEvent buildPendingEvent(
            Object entity,
            Class<?> entityClass,
            String tableName,
            OperationalEventOperation operation
    ) throws JsonProcessingException {
        String eventId = UUID.randomUUID().toString();
        Instant occurredAt = Instant.now();
        String entityId = entityId(entity);

        Map<String, Object> payload = new LinkedHashMap<>();
        payload.put("schema_version", PAYLOAD_SCHEMA_VERSION);
        payload.put("event_id", eventId);
        payload.put("occurred_at", UTC_FORMATTER.format(occurredAt));
        payload.put("operation", operation.name());
        payload.put("entity_name", entityClass.getName());
        payload.put("table_name", tableName);
        payload.put("entity_id", entityId);
        payload.put("columns", snapshotColumns(entity, entityClass, 0));

        return new PendingOperationalEvent(
                eventId,
                operation.name(),
                entityClass.getName(),
                tableName,
                entityId,
                Date.from(occurredAt),
                objectMapper.writeValueAsString(payload)
        );
    }

    private Map<String, Object> snapshotColumns(Object value, Class<?> entityClass, int depth) {
        Map<String, Object> snapshot = new LinkedHashMap<>();
        for (Field field : fieldsOf(entityClass)) {
            if (shouldSkipField(field)) {
                continue;
            }

            try {
                field.setAccessible(true);
                Object fieldValue = field.get(value);

                Column column = field.getAnnotation(Column.class);
                if (column != null) {
                    String columnName = columnName(column, field.getName());
                    if (!isSensitive(columnName) && !isSensitive(field.getName())) {
                        snapshot.put(columnName, normalizeValue(fieldValue));
                    }
                    continue;
                }

                JoinColumn joinColumn = field.getAnnotation(JoinColumn.class);
                if (joinColumn != null && (field.isAnnotationPresent(ManyToOne.class) || field.isAnnotationPresent(OneToOne.class))) {
                    String columnName = joinColumnName(joinColumn, field.getName());
                    if (!isSensitive(columnName) && !isSensitive(field.getName())) {
                        snapshot.put(columnName, entityId(fieldValue));
                    }
                    continue;
                }

                if (field.isAnnotationPresent(Embedded.class) && fieldValue != null && depth < MAX_EMBEDDED_DEPTH) {
                    snapshot.putAll(snapshotColumns(fieldValue, fieldValue.getClass(), depth + 1));
                }
            } catch (Exception e) {
                LOGGER.debug("Skipping operational event field snapshot for {}.{}", entityClass.getName(), field.getName(), e);
            }
        }
        return snapshot;
    }

    private void insertOutboxEvent(PendingOperationalEvent event) {
        Date now = new Date();
        jdbcTemplate.update(
                "INSERT INTO " + outboxTableName + " "
                        + "(EVENT_ID, OPERATION, ENTITY_NAME, TABLE_NAME, ENTITY_ID, OCCURRED_AT, CREATED_AT, ATTEMPT_COUNT, PAYLOAD) "
                        + "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
                event.eventId,
                event.operation,
                event.entityName,
                event.tableName,
                event.entityId,
                new Timestamp(event.occurredAt.getTime()),
                new Timestamp(now.getTime()),
                0,
                event.payload
        );

        if (LOGGER.isDebugEnabled()) {
            LOGGER.debug("Persisted operational outbox event table={} operation={} entityId={}",
                    event.tableName, event.operation, event.entityId);
        }
    }

    private Set<String> parseTrackedTables(String raw) {
        if (raw == null || raw.trim().isEmpty()) {
            return Collections.emptySet();
        }
        LinkedHashSet<String> parsed = Arrays.stream(raw.split(","))
                .map(String::trim)
                .filter(token -> !token.isEmpty())
                .map(token -> token.toUpperCase(Locale.ROOT))
                .collect(Collectors.toCollection(LinkedHashSet::new));
        return Collections.unmodifiableSet(parsed);
    }

    private String qualifyOutboxTableName(String configuredSchema) {
        if (configuredSchema == null || configuredSchema.trim().isEmpty()) {
            return "OPERATIONAL_EVENT_OUTBOX";
        }
        String schema = configuredSchema.trim();
        if (!schema.matches("[A-Za-z_][A-Za-z0-9_]*")) {
            LOGGER.warn("Ignoring invalid db.schema for operational outbox table qualification: {}", schema);
            return "OPERATIONAL_EVENT_OUTBOX";
        }
        return schema + ".OPERATIONAL_EVENT_OUTBOX";
    }

    private List<Field> fieldsOf(Class<?> type) {
        List<Field> fields = new ArrayList<>();
        Class<?> current = type;
        while (current != null && current != Object.class) {
            fields.addAll(Arrays.asList(current.getDeclaredFields()));
            current = current.getSuperclass();
        }
        return fields;
    }

    private boolean shouldSkipField(Field field) {
        int modifiers = field.getModifiers();
        return Modifier.isStatic(modifiers)
                || Modifier.isTransient(modifiers)
                || field.isAnnotationPresent(Transient.class)
                || isSensitive(field.getName());
    }

    private String columnName(Column column, String fallback) {
        return column.name() == null || column.name().trim().isEmpty()
                ? fallback.toUpperCase(Locale.ROOT)
                : column.name().trim().toUpperCase(Locale.ROOT);
    }

    private String joinColumnName(JoinColumn joinColumn, String fallback) {
        return joinColumn.name() == null || joinColumn.name().trim().isEmpty()
                ? fallback.toUpperCase(Locale.ROOT) + "_ID"
                : joinColumn.name().trim().toUpperCase(Locale.ROOT);
    }

    private boolean isSensitive(String name) {
        if (name == null) {
            return false;
        }
        String normalized = name.replace("-", "_").replace(" ", "_").toLowerCase(Locale.ROOT);
        for (String token : SENSITIVE_TOKENS) {
            if (normalized.contains(token)) {
                return true;
            }
        }
        return false;
    }

    private Object normalizeValue(Object value) {
        if (value == null) {
            return null;
        }
        if (value instanceof Date) {
            return UTC_FORMATTER.format(((Date) value).toInstant());
        }
        if (value instanceof Enum<?>) {
            return ((Enum<?>) value).name();
        }
        if (value instanceof BigDecimal || value instanceof BigInteger) {
            return value.toString();
        }
        if (value instanceof Number || value instanceof Boolean || value instanceof CharSequence) {
            return value;
        }
        return value.toString();
    }

    private String entityId(Object value) {
        if (value == null) {
            return null;
        }
        try {
            if (value instanceof SalesManagerEntity<?, ?>) {
                Object id = ((SalesManagerEntity<?, ?>) value).getId();
                return id == null ? null : id.toString();
            }
            Field idField = Hibernate.getClass(value).getDeclaredField("id");
            idField.setAccessible(true);
            Object id = idField.get(value);
            return id == null ? null : id.toString();
        } catch (Exception e) {
            LOGGER.debug("Could not resolve entity id for {}", value.getClass().getName(), e);
            return null;
        }
    }

    private static final class PendingOperationalEvent {
        private final String eventId;
        private final String operation;
        private final String entityName;
        private final String tableName;
        private final String entityId;
        private final Date occurredAt;
        private final String payload;

        private PendingOperationalEvent(
                String eventId,
                String operation,
                String entityName,
                String tableName,
                String entityId,
                Date occurredAt,
                String payload
        ) {
            this.eventId = eventId;
            this.operation = operation;
            this.entityName = entityName;
            this.tableName = tableName;
            this.entityId = entityId;
            this.occurredAt = new Date(occurredAt.getTime());
            this.payload = payload;
        }
    }
}
