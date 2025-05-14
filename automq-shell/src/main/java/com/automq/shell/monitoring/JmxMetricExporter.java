package io.automq.monitoring;

import io.prometheus.client.exporter.HTTPServer;
import io.prometheus.client.hotspot.DefaultExports;

import java.io.IOException;

public class JmxMetricsExporter {

    private static HTTPServer server;

    public static void start(int port) {
        try {
            DefaultExports.initialize(); // Collect JVM metrics
            server = new HTTPServer(port);
            System.out.println("Prometheus metrics exporter started at http://localhost:" + port + "/metrics");
        } catch (IOException e) {
            System.err.println("Failed to start Prometheus exporter: " + e.getMessage());
        }
    }

    public static void stop() {
        if (server != null) {
            server.stop();
            System.out.println("Prometheus exporter stopped.");
        }
    }
}
