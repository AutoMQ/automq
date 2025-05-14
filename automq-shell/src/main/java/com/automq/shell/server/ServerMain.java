import io.automq.monitoring.JmxMetricsExporter;

public class ServerMain {
    public static void main(String[] args) {
        JmxMetricsExporter.start(8080);  // Start the /metrics endpoint
    }
}
