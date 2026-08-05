package com.salesmanager.core.model.operational;

import java.io.Serializable;
import java.util.Date;

import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.GeneratedValue;
import javax.persistence.GenerationType;
import javax.persistence.Id;
import javax.persistence.Index;
import javax.persistence.Lob;
import javax.persistence.Table;
import javax.persistence.Temporal;
import javax.persistence.TemporalType;

@Entity
@Table(
        name = "OPERATIONAL_EVENT_OUTBOX",
        indexes = {
                @Index(name = "OP_EVT_OUTBOX_SENT_IDX", columnList = "SENT_AT"),
                @Index(name = "OP_EVT_OUTBOX_SENT_ID_IDX", columnList = "SENT_AT,OUTBOX_ID"),
                @Index(name = "OP_EVT_OUTBOX_TABLE_IDX", columnList = "TABLE_NAME"),
                @Index(name = "OP_EVT_OUTBOX_AGG_IDX", columnList = "TABLE_NAME,ENTITY_ID")
        }
)
public class OperationalEventOutbox implements Serializable {

    private static final long serialVersionUID = 1L;

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    @Column(name = "OUTBOX_ID", unique = true, nullable = false)
    private Long id;

    @Column(name = "EVENT_ID", nullable = false, unique = true, length = 64)
    private String eventId;

    @Column(name = "OPERATION", nullable = false, length = 16)
    private String operation;

    @Column(name = "ENTITY_NAME", nullable = false, length = 256)
    private String entityName;

    @Column(name = "TABLE_NAME", nullable = false, length = 128)
    private String tableName;

    @Column(name = "ENTITY_ID", length = 128)
    private String entityId;

    @Temporal(TemporalType.TIMESTAMP)
    @Column(name = "OCCURRED_AT", nullable = false)
    private Date occurredAt;

    @Temporal(TemporalType.TIMESTAMP)
    @Column(name = "CREATED_AT", nullable = false)
    private Date createdAt;

    @Temporal(TemporalType.TIMESTAMP)
    @Column(name = "SENT_AT")
    private Date sentAt;

    @Column(name = "ATTEMPT_COUNT", nullable = false)
    private int attemptCount = 0;

    @Lob
    @Column(name = "PAYLOAD", nullable = false)
    private String payload;

    @Lob
    @Column(name = "LAST_ERROR")
    private String lastError;

    public Long getId() {
        return id;
    }

    public void setId(Long id) {
        this.id = id;
    }

    public String getEventId() {
        return eventId;
    }

    public void setEventId(String eventId) {
        this.eventId = eventId;
    }

    public String getOperation() {
        return operation;
    }

    public void setOperation(String operation) {
        this.operation = operation;
    }

    public String getEntityName() {
        return entityName;
    }

    public void setEntityName(String entityName) {
        this.entityName = entityName;
    }

    public String getTableName() {
        return tableName;
    }

    public void setTableName(String tableName) {
        this.tableName = tableName;
    }

    public String getEntityId() {
        return entityId;
    }

    public void setEntityId(String entityId) {
        this.entityId = entityId;
    }

    public Date getOccurredAt() {
        return occurredAt == null ? null : new Date(occurredAt.getTime());
    }

    public void setOccurredAt(Date occurredAt) {
        this.occurredAt = occurredAt == null ? null : new Date(occurredAt.getTime());
    }

    public Date getCreatedAt() {
        return createdAt == null ? null : new Date(createdAt.getTime());
    }

    public void setCreatedAt(Date createdAt) {
        this.createdAt = createdAt == null ? null : new Date(createdAt.getTime());
    }

    public Date getSentAt() {
        return sentAt == null ? null : new Date(sentAt.getTime());
    }

    public void setSentAt(Date sentAt) {
        this.sentAt = sentAt == null ? null : new Date(sentAt.getTime());
    }

    public int getAttemptCount() {
        return attemptCount;
    }

    public void setAttemptCount(int attemptCount) {
        this.attemptCount = attemptCount;
    }

    public String getPayload() {
        return payload;
    }

    public void setPayload(String payload) {
        this.payload = payload;
    }

    public String getLastError() {
        return lastError;
    }

    public void setLastError(String lastError) {
        this.lastError = lastError;
    }
}
