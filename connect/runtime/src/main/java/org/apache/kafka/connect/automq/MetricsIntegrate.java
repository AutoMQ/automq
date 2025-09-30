package org.apache.kafka.connect.automq;

import com.automq.opentelemetry.AutoMQTelemetryManager;

public class MetricsIntegrate {
    
    AutoMQTelemetryManager autoMQTelemetryManager;
    
    public MetricsIntegrate(AutoMQTelemetryManager autoMQTelemetryManager) {
        this.autoMQTelemetryManager = autoMQTelemetryManager;
    }
}
