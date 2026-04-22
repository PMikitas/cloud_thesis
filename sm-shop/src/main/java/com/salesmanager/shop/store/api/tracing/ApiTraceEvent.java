package com.salesmanager.shop.store.api.tracing;

import java.time.Instant;

public class ApiTraceEvent {

    private final String eventId;
    private final String requestId;
    private final Instant eventTimestamp;
    private final String apiName;
    private final String endpoint;
    private final String statusCode;
    private final String errorType;
    private final String userId;
    private final String clientId;
    private final String sessionId;
    private final String payload;

    private ApiTraceEvent(Builder builder) {
        this.eventId = builder.eventId;
        this.requestId = builder.requestId;
        this.eventTimestamp = builder.eventTimestamp;
        this.apiName = builder.apiName;
        this.endpoint = builder.endpoint;
        this.statusCode = builder.statusCode;
        this.errorType = builder.errorType;
        this.userId = builder.userId;
        this.clientId = builder.clientId;
        this.sessionId = builder.sessionId;
        this.payload = builder.payload;
    }

    public String getEventId() { return eventId; }
    public String getRequestId() { return requestId; }
    public Instant getEventTimestamp() { return eventTimestamp; }
    public String getApiName() { return apiName; }
    public String getEndpoint() { return endpoint; }
    public String getStatusCode() { return statusCode; }
    public String getErrorType() { return errorType; }
    public String getUserId() { return userId; }
    public String getClientId() { return clientId; }
    public String getSessionId() { return sessionId; }
    public String getPayload() { return payload; }

    public static Builder builder() {
        return new Builder();
    }

    public static final class Builder {
        private String eventId;
        private String requestId;
        private Instant eventTimestamp;
        private String apiName;
        private String endpoint;
        private String statusCode;
        private String errorType;
        private String userId;
        private String clientId;
        private String sessionId;
        private String payload;

        public Builder eventId(String eventId) { this.eventId = eventId; return this; }
        public Builder requestId(String requestId) { this.requestId = requestId; return this; }
        public Builder eventTimestamp(Instant eventTimestamp) { this.eventTimestamp = eventTimestamp; return this; }
        public Builder apiName(String apiName) { this.apiName = apiName; return this; }
        public Builder endpoint(String endpoint) { this.endpoint = endpoint; return this; }
        public Builder statusCode(String statusCode) { this.statusCode = statusCode; return this; }
        public Builder errorType(String errorType) { this.errorType = errorType; return this; }
        public Builder userId(String userId) { this.userId = userId; return this; }
        public Builder clientId(String clientId) { this.clientId = clientId; return this; }
        public Builder sessionId(String sessionId) { this.sessionId = sessionId; return this; }
        public Builder payload(String payload) { this.payload = payload; return this; }

        public ApiTraceEvent build() { return new ApiTraceEvent(this); }
    }
}
