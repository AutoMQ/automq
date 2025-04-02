package kafka.log.stream.s3.telemetry.exporter;

import org.apache.kafka.common.utils.Utils;

import com.automq.stream.utils.URIUtils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.URI;
import java.net.URISyntaxException;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * Kafka Export URI format:
 * <pre>{@code
 * kafka://?bootstrapServers=host1:port1;host2:port2
 *          &topic=metrics
 *          &securityProtocol=PLAINTEXT
 *          &saslMechanism=PLAIN
 *          &saslUsername=admin
 *          &saslPassword=secret
 * }</pre>
 *
 * <h3>Parameter Description:</h3>
 * <ul>
 *   <li><b>bootstrapServers</b> - Kafka cluster address, required</li>
 *   <li><b>topic</b> - Kafka topic to which metrics are written, required</li>
 *   <li><b>securityProtocol</b> - Security protocol (default: PLAINTEXT)</li>
 *   <li><b>saslMechanism</b> - SASL authentication mechanism (default: PLAIN)</li>
 *   <li><b>saslUsername</b> - SASL username (required when using SASL authentication)</li>
 *   <li><b>saslPassword</b> - SASL password (required when using SASL authentication)</li>
 * </ul>
 */
public final class KafkaExportURI {
    private static final Logger LOGGER = LoggerFactory.getLogger(KafkaExportURI.class);
    private static final String DEFAULT_SECURITY_PROTOCOL = "PLAINTEXT";
    private static final String DEFAULT_SASL_MECHANISM = "PLAIN";
    private static final String BOOTSTRAP_SERVERS_KEY = "bootstrapServers";
    private static final String TOPIC_KEY = "topic";
    private static final String SECURITY_PROTOCOL_KEY = "securityProtocol";
    private static final String SASL_MECHANISM_KEY = "saslMechanism";
    private static final String SASL_USERNAME_KEY = "saslUsername";
    private static final String SASL_PASSWORD_KEY = "saslPassword";

    private final String bootstrapServers;
    private final String topic;
    private final String securityProtocol;
    private final String saslMechanism;
    private final String saslUsername;
    private final String saslPassword;

    /**
     * Constructor
     *
     * @param bootstrapServers Kafka cluster address, required
     * @param topic            Kafka topic to which metrics are written, required
     * @param securityProtocol Security protocol (default: PLAINTEXT)
     * @param saslMechanism    SASL authentication mechanism (default: PLAIN)
     * @param saslUsername     SASL username (required when using SASL authentication)
     * @param saslPassword     SASL password (required when using SASL authentication)
     */
    public KafkaExportURI(
            String bootstrapServers,
            String topic,
            String securityProtocol,
            String saslMechanism,
            String saslUsername,
            String saslPassword
    ) {
        validate(bootstrapServers, topic, securityProtocol, saslMechanism, saslUsername, saslPassword);
        this.bootstrapServers = bootstrapServers;
        this.topic = topic;
        this.securityProtocol = securityProtocol != null ? securityProtocol : DEFAULT_SECURITY_PROTOCOL;
        this.saslMechanism = saslMechanism != null ? saslMechanism : DEFAULT_SASL_MECHANISM;
        this.saslUsername = saslUsername;
        this.saslPassword = saslPassword;
    }

    // Getters keep the same access way as a record
    public String bootstrapServers() {
        return bootstrapServers;
    }

    public String topic() {
        return topic;
    }

    public String securityProtocol() {
        return securityProtocol;
    }

    public String saslMechanism() {
        return saslMechanism;
    }

    public String saslUsername() {
        return saslUsername;
    }

    public String saslPassword() {
        return saslPassword;
    }

    private static void validate(String bootstrapServers, String topic, String securityProtocol,
                                 String saslMechanism, String saslUsername, String saslPassword) {
        // Keep the original validation logic...
        if (Utils.isBlank(bootstrapServers)) {
            throw new IllegalArgumentException("bootstrapServers must be specified");
        }
        if (Utils.isBlank(topic)) {
            throw new IllegalArgumentException("topic must be specified");
        }
        if (securityProtocol != null && securityProtocol.startsWith("SASL_")) {
            if (Utils.isBlank(saslUsername) || Utils.isBlank(saslPassword)) {
                throw new IllegalArgumentException(
                        "saslUsername and saslPassword must be specified when using SASL security protocol");
            }
            if (!"PLAIN".equals(saslMechanism) && !"SCRAM-SHA-256".equals(saslMechanism) && !"SCRAM-SHA-512".equals(saslMechanism)) {
                throw new IllegalArgumentException("Invalid SASL mechanism: " + saslMechanism);
            }
        }
    }

    // Keep the original static factory method
    public static KafkaExportURI parse(String uriStr) {
        try {
            URI uri = new URI(uriStr);
            Map<String, List<String>> queries = URIUtils.splitQuery(uri);

            String bootstrapServers = URIUtils.getString(queries, BOOTSTRAP_SERVERS_KEY, "");
            String topic = URIUtils.getString(queries, TOPIC_KEY, "");
            String securityProtocol = URIUtils.getString(queries, SECURITY_PROTOCOL_KEY, DEFAULT_SECURITY_PROTOCOL);
            String saslMechanism = URIUtils.getString(queries, SASL_MECHANISM_KEY, DEFAULT_SASL_MECHANISM);
            String saslUsername = URIUtils.getString(queries, SASL_USERNAME_KEY, "");
            String saslPassword = URIUtils.getString(queries, SASL_PASSWORD_KEY, "");

            return new KafkaExportURI(
                    bootstrapServers,
                    topic,
                    securityProtocol,
                    saslMechanism,
                    saslUsername,
                    saslPassword
            );
        } catch (URISyntaxException e) {
            LOGGER.error("Invalid Kafka export URI: {}", uriStr, e);
            throw new IllegalArgumentException("Invalid Kafka export URI: " + uriStr);
        }
    }

    // Override equals/hashCode/toString to keep behavior similar to a record
    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        KafkaExportURI that = (KafkaExportURI) o;
        return Objects.equals(bootstrapServers, that.bootstrapServers) &&
                Objects.equals(topic, that.topic) &&
                Objects.equals(securityProtocol, that.securityProtocol) &&
                Objects.equals(saslMechanism, that.saslMechanism) &&
                Objects.equals(saslUsername, that.saslUsername) &&
                Objects.equals(saslPassword, that.saslPassword);
    }

    @Override
    public int hashCode() {
        return Objects.hash(bootstrapServers, topic, securityProtocol, saslMechanism, saslUsername, saslPassword);
    }

    @Override
    public String toString() {
        return "KafkaExportURI[" +
                "bootstrapServers=" + bootstrapServers + ", " +
                "topic=" + topic + ", " +
                "securityProtocol=" + securityProtocol + ", " +
                "saslMechanism=" + saslMechanism + ", " +
                "saslUsername=" + saslUsername + ", " +
                "saslPassword=" + (saslPassword != null ? "******" : null) +
                ']';
    }
}
