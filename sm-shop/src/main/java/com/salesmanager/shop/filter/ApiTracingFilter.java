package com.salesmanager.shop.filter;

import com.salesmanager.shop.store.api.tracing.ApiTraceEvent;
import com.salesmanager.shop.store.api.tracing.BigQueryTracingService;
import com.salesmanager.shop.store.api.tracing.RedshiftTracingService;
import com.salesmanager.shop.store.api.tracing.SnowflakeTracingService;
import com.salesmanager.shop.store.api.tracing.TracingDestinationConfig;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.core.annotation.Order;
import org.springframework.security.core.Authentication;
import org.springframework.security.core.context.SecurityContextHolder;
import org.springframework.stereotype.Component;
import org.springframework.web.util.ContentCachingRequestWrapper;

import javax.servlet.Filter;
import javax.servlet.FilterChain;
import javax.servlet.FilterConfig;
import javax.servlet.ServletException;
import javax.servlet.ServletRequest;
import javax.servlet.ServletResponse;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import javax.servlet.http.HttpServletResponseWrapper;
import javax.servlet.http.HttpSession;
import java.io.IOException;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.Base64;
import java.util.UUID;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Servlet filter that traces every API request to external analytics sinks.
 *
 * Runs after XssFilter (Order 0) so the request is already sanitised.
 * Only intercepts paths that start with /api/ to avoid noise from static
 * resources and actuator endpoints.
 *
 * BigQuery / Redshift row schema:
 *   event_id       STRING   – unique trace event UUID
 *   request_id     STRING   – per-request UUID (or value of X-Request-Id header)
 *   event_timestamp TIMESTAMP – UTC timestamp of the request
 *   api_name       STRING   – logical API name derived from the URL resource
 *   endpoint       STRING   – "METHOD /path/template"
 *   status_code    STRING   – HTTP response status code
 *   error_type     STRING   – error classification or empty for 2xx
*   user_id        STRING   – authenticated principal name or empty
 *   client_id      STRING   – value of X-Client-Id header or empty
 *   session_id     STRING   – value of X-Session-Id header or HttpSession id
 *   payload        STRING   – JSON request body (or {"query":"…"} for GET/DELETE);
 *                              filterable via JSON_VALUE(payload, '$.field')
 */
@Component
@Order(1)
public class ApiTracingFilter implements Filter {

    private static final Logger LOGGER = LoggerFactory.getLogger(ApiTracingFilter.class);

    private static final Pattern API_RESOURCE_PATTERN =
            Pattern.compile("/api/v\\d+/(?:private/|auth/)?([^/?]+)");

    /** Cap stored body size to keep BigQuery rows lean. */
    private static final int MAX_PAYLOAD_BYTES = 16 * 1024;

    @Autowired
    private BigQueryTracingService bigQueryTracingService;

    @Autowired
    private RedshiftTracingService redshiftTracingService;

    @Autowired
    private SnowflakeTracingService snowflakeTracingService;

    @Autowired
    private TracingDestinationConfig tracingDestinationConfig;

    @Override
    public void init(FilterConfig filterConfig) throws ServletException {
        LOGGER.debug("ApiTracingFilter initialized");
    }

    @Override
    public void doFilter(ServletRequest servletRequest, ServletResponse servletResponse, FilterChain chain)
            throws IOException, ServletException {

        HttpServletRequest request = (HttpServletRequest) servletRequest;
        HttpServletResponse response = (HttpServletResponse) servletResponse;

        String uri = request.getRequestURI();
        if (!uri.startsWith("/api/")) {
            chain.doFilter(request, response);
            return;
        }

        StatusCapturingResponseWrapper wrappedResponse = new StatusCapturingResponseWrapper(response);
        ContentCachingRequestWrapper wrappedRequest = new ContentCachingRequestWrapper(request);
        Instant requestTime = Instant.now();

        try {
            chain.doFilter(wrappedRequest, wrappedResponse);
        } finally {
            try {
                int statusCode = wrappedResponse.getStatus();
                ApiTraceEvent event = ApiTraceEvent.builder()
                        .eventId(UUID.randomUUID().toString())
                        .requestId(resolveRequestId(request))
                        .eventTimestamp(requestTime)
                        .apiName(extractApiName(uri))
                        .endpoint(request.getMethod() + " " + uri)
                        .statusCode(String.valueOf(statusCode))
                        .errorType(classifyError(statusCode))
                        .userId(resolveUserId(request, wrappedRequest))
                        .clientId(resolveClientId(request))
                        .sessionId(resolveSessionId(request))
                        .payload(extractPayload(wrappedRequest))
                        .build();

                if (tracingDestinationConfig.isApiEventDestinationEnabled("bigquery")) {
                    bigQueryTracingService.traceAsync(event);
                }
                if (tracingDestinationConfig.isApiEventDestinationEnabled("redshift")) {
                    redshiftTracingService.traceAsync(event);
                }
                if (tracingDestinationConfig.isApiEventDestinationEnabled("snowflake")) {
                    snowflakeTracingService.traceAsync(event);
                }
            } catch (Exception e) {
                LOGGER.error("Failed to build or dispatch API trace event", e);
            }
        }
    }

    @Override
    public void destroy() {
        LOGGER.debug("ApiTracingFilter destroyed");
    }

    // ── Helpers ──────────────────────────────────────────────────────────────

    private String resolveRequestId(HttpServletRequest request) {
        String header = request.getHeader("X-Request-Id");
        return StringUtils.isNotBlank(header) ? header : UUID.randomUUID().toString();
    }

    private String resolveClientId(HttpServletRequest request) {
        String clientId = request.getHeader("X-Client-Id");
        return StringUtils.isNotBlank(clientId) ? clientId : "";
    }

    private String resolveSessionId(HttpServletRequest request) {
        String header = request.getHeader("X-Session-Id");
        if (StringUtils.isNotBlank(header)) {
            return header;
        }
        try {
            HttpSession session = request.getSession(false);
            if (session != null) {
                return session.getId();
            }
        } catch (Exception e) {
            LOGGER.debug("Could not resolve HTTP session id", e);
        }
        return "";
    }

    /**
     * Resolves a non-empty user identifier for every traced request. Sources
     * are tried in priority order:
     *   1. Spring SecurityContext — populated for JWT-authed endpoints, but
     *      may already be cleared by the time this filter's finally-block
     *      runs (SecurityContextPersistenceFilter clears it inside the chain).
     *   2. Authorization: Bearer JWT — we base64-decode the payload segment
     *      and read `sub` / `user_name` / `username`. Signature is NOT
     *      verified (tracing is non-authoritative).
     *   3. Login/register request body — pulls `username` or `emailAddress`
     *      from the cached JSON so newcomers get a row with their email even
     *      before a token exists.
     *   4. Anonymous fallback — prefixes the session id with "anon:" so every
     *      row has something queryable; later events on the same session can
     *      be stitched to a real user via session_id.
     */
    private String resolveUserId(HttpServletRequest request, ContentCachingRequestWrapper bodyRequest) {
        String fromContext = resolveUserIdFromSecurityContext();
        if (StringUtils.isNotBlank(fromContext)) return fromContext;

        String fromJwt = resolveUserIdFromJwt(request);
        if (StringUtils.isNotBlank(fromJwt)) return fromJwt;

        String fromBody = resolveUserIdFromLoginOrRegisterBody(request, bodyRequest);
        if (StringUtils.isNotBlank(fromBody)) return fromBody;

        String sessionId = resolveSessionId(request);
        return StringUtils.isNotBlank(sessionId) ? "anon:" + sessionId : "anonymous";
    }

    private String resolveUserIdFromSecurityContext() {
        try {
            Authentication auth = SecurityContextHolder.getContext().getAuthentication();
            if (auth != null && auth.isAuthenticated() && !"anonymousUser".equals(auth.getPrincipal())) {
                return auth.getName();
            }
        } catch (Exception e) {
            LOGGER.debug("Could not resolve user from SecurityContext", e);
        }
        return "";
    }

    private String resolveUserIdFromJwt(HttpServletRequest request) {
        String auth = request.getHeader("Authorization");
        if (auth == null || auth.length() < 8) return "";
        String lower = auth.toLowerCase();
        if (!lower.startsWith("bearer ")) return "";
        String token = auth.substring(7).trim();
        String[] parts = token.split("\\.");
        if (parts.length < 2) return "";
        try {
            byte[] decoded = Base64.getUrlDecoder().decode(parts[1]);
            String payload = new String(decoded, StandardCharsets.UTF_8);
            String sub = extractJsonString(payload, "sub");
            if (StringUtils.isNotBlank(sub)) return sub;
            String userName = extractJsonString(payload, "user_name");
            if (StringUtils.isNotBlank(userName)) return userName;
            return extractJsonString(payload, "username");
        } catch (Exception e) {
            LOGGER.debug("Could not decode JWT payload for user id", e);
            return "";
        }
    }

    private String resolveUserIdFromLoginOrRegisterBody(HttpServletRequest request, ContentCachingRequestWrapper bodyRequest) {
        String uri = request.getRequestURI();
        if (!uri.contains("/customer/login") && !uri.contains("/customer/register") && !uri.contains("/private/login")) {
            return "";
        }
        try {
            byte[] body = bodyRequest.getContentAsByteArray();
            if (body == null || body.length == 0) return "";
            String json = new String(body, StandardCharsets.UTF_8);
            String username = extractJsonString(json, "username");
            if (StringUtils.isNotBlank(username)) return username;
            return extractJsonString(json, "emailAddress");
        } catch (Exception e) {
            LOGGER.debug("Could not read login/register body for user id", e);
            return "";
        }
    }

    /** Cheap zero-dep JSON string extraction: finds "key":"value" without pulling in a parser. */
    private String extractJsonString(String json, String key) {
        String needle = "\"" + key + "\"";
        int idx = json.indexOf(needle);
        if (idx < 0) return "";
        int colon = json.indexOf(':', idx + needle.length());
        if (colon < 0) return "";
        int start = json.indexOf('"', colon + 1);
        if (start < 0) return "";
        int end = start + 1;
        while (end < json.length() && json.charAt(end) != '"') {
            if (json.charAt(end) == '\\' && end + 1 < json.length()) end++;
            end++;
        }
        return end > start + 1 && end <= json.length() ? json.substring(start + 1, Math.min(end, json.length())) : "";
    }

    private String extractApiName(String uri) {
        Matcher m = API_RESOURCE_PATTERN.matcher(uri);
        if (m.find()) {
            String resource = m.group(1);
            return Character.toUpperCase(resource.charAt(0)) + resource.substring(1) + "Api";
        }
        return "UnknownApi";
    }

    /**
     * Returns the request body as a JSON string when the content-type is JSON,
     * or wraps the query string into a JSON envelope for GET/DELETE so every
     * row carries a parseable JSON payload (queryable in BigQuery via JSON_VALUE).
     */
    private String extractPayload(ContentCachingRequestWrapper request) {
        try {
            byte[] body = request.getContentAsByteArray();
            String contentType = request.getContentType();
            if (body != null && body.length > 0 && contentType != null
                    && contentType.toLowerCase().contains("json")) {
                Charset cs = request.getCharacterEncoding() != null
                        ? Charset.forName(request.getCharacterEncoding())
                        : StandardCharsets.UTF_8;
                int len = Math.min(body.length, MAX_PAYLOAD_BYTES);
                return new String(body, 0, len, cs);
            }
            String qs = request.getQueryString();
            if (StringUtils.isNotBlank(qs)) {
                return "{\"query\":\"" + qs.replace("\\", "\\\\").replace("\"", "\\\"") + "\"}";
            }
        } catch (Exception e) {
            LOGGER.debug("Could not extract request payload", e);
        }
        return "";
    }

    private String classifyError(int statusCode) {
        if (statusCode >= 200 && statusCode < 300) return "";
        switch (statusCode) {
            case 400: return "BAD_REQUEST";
            case 401: return "UNAUTHORIZED";
            case 403: return "FORBIDDEN";
            case 404: return "NOT_FOUND";
            case 409: return "CONFLICT";
            case 422: return "UNPROCESSABLE_ENTITY";
            case 429: return "TOO_MANY_REQUESTS";
            default:
                if (statusCode >= 400 && statusCode < 500) return "CLIENT_ERROR";
                if (statusCode >= 500) return "SERVER_ERROR";
                return "";
        }
    }

    // ── Inner class ───────────────────────────────────────────────────────────

    private static final class StatusCapturingResponseWrapper extends HttpServletResponseWrapper {

        private int status = 200;

        StatusCapturingResponseWrapper(HttpServletResponse response) {
            super(response);
        }

        @Override
        public void setStatus(int sc) {
            this.status = sc;
            super.setStatus(sc);
        }

        @Override
        @SuppressWarnings("deprecation")
        public void setStatus(int sc, String sm) {
            this.status = sc;
            super.setStatus(sc, sm);
        }

        @Override
        public void sendError(int sc) throws IOException {
            this.status = sc;
            super.sendError(sc);
        }

        @Override
        public void sendError(int sc, String msg) throws IOException {
            this.status = sc;
            super.sendError(sc, msg);
        }

        @Override
        public int getStatus() {
            return status;
        }
    }
}
