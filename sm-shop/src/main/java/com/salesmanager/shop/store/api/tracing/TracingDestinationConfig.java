package com.salesmanager.shop.store.api.tracing;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.Locale;
import java.util.Map;
import java.util.Set;

@Component
public class TracingDestinationConfig {

    private static final Logger LOGGER = LoggerFactory.getLogger(TracingDestinationConfig.class);

    private static final Set<String> SUPPORTED_DESTINATIONS = Collections.unmodifiableSet(
            new LinkedHashSet<>(Arrays.asList("bigquery", "snowflake", "redshift"))
    );

    private final Map<String, String> fileValues;
    private final boolean apiEventsEnabled;
    private final Set<String> apiEventDestinations;

    public TracingDestinationConfig() {
        this.fileValues = loadTracingEnvFile();
        this.apiEventsEnabled = readBoolean(
                "TRACING_ENABLED_API_EVENTS",
                "tracing.enabled.api.events",
                true
        );
        this.apiEventDestinations = parseDestinations(readValue(
                "TRACING_API_EVENTS_CLOUD_SOLUTIONS",
                "tracing.api.events.cloud.solutions",
                "snowflake"
        ));

        LOGGER.info(
                "API event tracing config resolved: enabled={}, destinations={}",
                apiEventsEnabled,
                apiEventDestinations
        );
    }

    public boolean isApiEventDestinationEnabled(String destination) {
        return apiEventsEnabled && apiEventDestinations.contains(normalize(destination));
    }

    public Set<String> getApiEventDestinations() {
        return apiEventDestinations;
    }

    private boolean readBoolean(String envName, String propertyName, boolean defaultValue) {
        String value = readValue(envName, propertyName, Boolean.toString(defaultValue));
        return !"false".equalsIgnoreCase(value)
                && !"0".equals(value)
                && !"no".equalsIgnoreCase(value)
                && !"off".equalsIgnoreCase(value);
    }

    private String readValue(String envName, String propertyName, String defaultValue) {
        String value = firstNonBlank(
                System.getenv(envName),
                System.getProperty(propertyName),
                fileValues.get(envName),
                fileValues.get(propertyName)
        );
        return value != null ? value : defaultValue;
    }

    private Set<String> parseDestinations(String rawValue) {
        LinkedHashSet<String> parsed = new LinkedHashSet<>();
        if (rawValue == null) {
            return Collections.emptySet();
        }

        for (String token : rawValue.split(",")) {
            String destination = normalize(token);
            if (destination.isEmpty()) {
                continue;
            }
            if (!SUPPORTED_DESTINATIONS.contains(destination)) {
                LOGGER.warn("Ignoring unsupported API event tracing destination '{}'", token.trim());
                continue;
            }
            parsed.add(destination);
        }

        return Collections.unmodifiableSet(parsed);
    }

    private Map<String, String> loadTracingEnvFile() {
        for (Path candidate : tracingEnvFileCandidates()) {
            if (!Files.isRegularFile(candidate)) {
                continue;
            }
            try {
                Map<String, String> values = new HashMap<>();
                for (String rawLine : Files.readAllLines(candidate, StandardCharsets.UTF_8)) {
                    String line = rawLine.trim();
                    if (line.isEmpty() || line.startsWith("#") || !line.contains("=")) {
                        continue;
                    }
                    String[] parts = line.split("=", 2);
                    values.put(parts[0].trim(), parts[1].trim());
                }
                LOGGER.info("Loaded central tracing config from {}", candidate.toAbsolutePath());
                return values;
            } catch (IOException e) {
                LOGGER.warn("Could not read central tracing config from {}", candidate.toAbsolutePath(), e);
            }
        }
        return Collections.emptyMap();
    }

    private Iterable<Path> tracingEnvFileCandidates() {
        LinkedHashSet<Path> candidates = new LinkedHashSet<>();
        String configured = firstNonBlank(
                System.getenv("TRACING_CONFIG_PATH"),
                System.getProperty("tracing.config.path")
        );
        if (configured != null) {
            candidates.add(Paths.get(configured).toAbsolutePath().normalize());
        }
        candidates.add(Paths.get("tracing.env").toAbsolutePath().normalize());
        candidates.add(Paths.get("../tracing.env").toAbsolutePath().normalize());
        candidates.add(Paths.get("/config/tracing.env"));
        return candidates;
    }

    private String firstNonBlank(String... values) {
        for (String value : values) {
            if (value != null && !value.trim().isEmpty()) {
                return value.trim();
            }
        }
        return null;
    }

    private String normalize(String value) {
        return value == null ? "" : value.trim().toLowerCase(Locale.ROOT);
    }
}
