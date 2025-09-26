package com.automq.opentelemetry;

import org.junit.jupiter.api.Test;

import java.util.Map;
import java.util.Properties;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;

class TelemetryConfigTest {

    @Test
    void getPropertiesWithPrefixStripsPrefixAndIgnoresOthers() {
        Properties properties = new Properties();
        properties.setProperty("automq.telemetry.s3.selector.type", "kafka");
        properties.setProperty("automq.telemetry.s3.selector.kafka.bootstrap.servers", "localhost:9092");
        properties.setProperty("automq.telemetry.s3.selector.kafka.security.protocol", "SASL_PLAINTEXT");
        properties.setProperty("unrelated.key", "value");

        TelemetryConfig config = new TelemetryConfig(properties);
        Map<String, String> result = config.getPropertiesWithPrefix("automq.telemetry.s3.selector.");

        assertEquals("kafka", result.get("type"));
        assertEquals("localhost:9092", result.get("kafka.bootstrap.servers"));
        assertEquals("SASL_PLAINTEXT", result.get("kafka.security.protocol"));
        assertFalse(result.containsKey("unrelated.key"));
    }
}
