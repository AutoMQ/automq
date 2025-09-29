package kafka.automq.table;

import kafka.server.KafkaConfig;

import org.apache.kafka.raft.QuorumConfig;
import org.apache.kafka.server.config.KRaftConfigs;

import com.sun.net.httpserver.HttpServer;

import org.apache.iceberg.aws.s3.S3FileIOProperties;
import org.apache.iceberg.inmemory.InMemoryFileIO;
import org.apache.iceberg.rest.RESTCatalog;
import org.apache.iceberg.util.SerializableMap;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.stream.Stream;

import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.stream.Collectors.toMap;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.TestInstance.Lifecycle.PER_CLASS;

@TestInstance(PER_CLASS)
class CatalogFactoryTest {
    // minimalistic properties to let KafkaConfig validation pass and let us test our catalog factory
    private final Map<String, String> requiredKafkaConfigProperties = Map.of(
        KRaftConfigs.CONTROLLER_LISTENER_NAMES_CONFIG, "PLAINTEXT",
        KRaftConfigs.NODE_ID_CONFIG, "2",
        KRaftConfigs.PROCESS_ROLES_CONFIG, "controller",
        QuorumConfig.QUORUM_VOTERS_CONFIG, "2@localhost:9092"
    );

    @Test
    void restPassthroughProperties() throws IOException {
        final var restCatalog = new RestCatalogMock();
        try (final var autoClose = restCatalog) {
            final var config = new KafkaConfig(merge(requiredKafkaConfigProperties, Map.of(
                "automq.table.topic.catalog.type", "rest",
                "automq.table.topic.catalog.uri", restCatalog.base(),
                "automq.table.topic.catalog.header.x-custom", "my-x", // Apache Polaris needs a tenant header for ex
                // automq specific/enforced (not standard catalog passthrough)
                "s3.data.buckets", "0@s3://my_bucket?region=us-east-1&endpoint=http://localhost:12345&pathStyle=true"
            )));
            final var catalog = new CatalogFactory.Builder(config).build();
            assertInstanceOf(RESTCatalog.class, catalog).close();
        }
        assertEquals(List.of("GET /v1/config?warehouse=s3://my_bucket/iceberg\nmy-x"), restCatalog.requests());
    }

    @Test
    void ignoreEmptyS3EndpointForRestCatalog() throws IOException {
        FakeS3IO.lastS3FileIOProperties = null;
        try (final var restCatalog = new RestCatalogMock()) {
            final var config = new KafkaConfig(merge(requiredKafkaConfigProperties, Map.of(
                "automq.table.topic.catalog.type", "rest",
                "automq.table.topic.catalog.uri", restCatalog.base(),
                "automq.table.topic.catalog.io-impl", FakeS3IO.class.getName(),
                "s3.data.buckets", "0@s3://my_bucket?region=us-east-1"
            )));
            final var catalog = new CatalogFactory.Builder(config).build();
            assertInstanceOf(RESTCatalog.class, catalog).close();
            assertNull(FakeS3IO.lastS3FileIOProperties().endpoint(), "S3FileIO endpoint should be null when not set - not even empty");
        } finally {
            FakeS3IO.lastS3FileIOProperties = null;
        }
    }

    @SafeVarargs
    private Map<String, String> merge(final Map<String, String>... all) {
        return Stream.of(all)
            .flatMap(it -> it.entrySet().stream())
            .collect(toMap(Map.Entry::getKey, Map.Entry::getValue, (a, b) -> b));
    }

    public static class FakeS3IO extends InMemoryFileIO {
        private static S3FileIOProperties lastS3FileIOProperties;

        private static S3FileIOProperties lastS3FileIOProperties() {
            return lastS3FileIOProperties;
        }

        @Override
        public void initialize(final Map<String, String> properties) {
            lastS3FileIOProperties = new S3FileIOProperties(SerializableMap.copyOf(properties));
            super.initialize(properties);
        }
    }

    private static class RestCatalogMock implements AutoCloseable {
        private final List<String> requests = new CopyOnWriteArrayList<>(); // normally overkill but makes the test more accurate
        private final HttpServer catalogBackend;

        private RestCatalogMock() throws IOException {
            catalogBackend = HttpServer.create(new InetSocketAddress("localhost", 0), 16);
            catalogBackend.createContext("/").setHandler(ex -> {
                try (ex) {
                    final var method = ex.getRequestMethod();
                    requests.add(
                        method + ' ' + ex.getRequestURI().getPath() + '?' + ex.getRequestURI().getQuery() +
                            ('\n' + String.join("", ex.getRequestHeaders().getOrDefault("x-custom", List.of()))) +
                            ('\n' + new String(ex.getRequestBody().readAllBytes(), UTF_8)).strip());

                    if (method.equals("GET") &&
                        ex.getRequestURI().getPath().equals("/v1/config") &&
                        "warehouse=s3%3A%2F%2Fmy_bucket%2Ficeberg".equals(ex.getRequestURI().getRawQuery())) {
                        final var body = """
                            {
                              "defaults": {},
                              "overrides": {}
                            }
                            """.getBytes(UTF_8);
                        ex.getResponseHeaders().add("content-type", "application/json");
                        ex.sendResponseHeaders(200, body.length);
                        ex.getResponseBody().write(body);
                        return;
                    }

                    // else we just called an unexpected endpoint, issue a HTTP 404
                    ex.sendResponseHeaders(404, 0);
                }
            });
            catalogBackend.start();
        }

        private String base() {
            return "http://localhost:" + catalogBackend.getAddress().getPort();
        }

        private List<String> requests() {
            return requests;
        }

        @Override
        public void close() {
            catalogBackend.stop(0);
        }
    }
}
