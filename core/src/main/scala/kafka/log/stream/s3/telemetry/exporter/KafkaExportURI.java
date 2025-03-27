package kafka.log.stream.s3.telemetry.exporter;

import com.automq.stream.utils.URIUtils;
import org.apache.kafka.common.utils.Utils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.URI;
import java.net.URISyntaxException;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * Kafka Export URI 格式：
 * <pre>{@code
 * kafka://?bootstrapServers=host1:port1;host2:port2
 *          &topic=metrics
 *          &securityProtocol=PLAINTEXT
 *          &saslMechanism=PLAIN
 *          &saslUsername=admin
 *          &saslPassword=secret
 * }</pre>
 *
 * <h3>参数说明：</h3>
 * <ul>
 *   <li><b>bootstrapServers</b> - Kafka 集群地址，必填</li>
 *   <li><b>topic</b> - 指标写入的 Kafka topic，必填</li>
 *   <li><b>securityProtocol</b> - 安全协议（默认 PLAINTEXT）</li>
 *   <li><b>saslMechanism</b> - SASL 认证机制（默认 PLAIN）</li>
 *   <li><b>saslUsername</b> - SASL 用户名（当使用 SASL 认证时必填）</li>
 *   <li><b>saslPassword</b> - SASL 密码（当使用 SASL 认证时必填）</li>
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
     * 构造方法
     * @param bootstrapServers Kafka 集群地址，必填
     * @param topic            指标写入的 Kafka topic，必填
     * @param securityProtocol 安全协议（默认 PLAINTEXT）
     * @param saslMechanism    SASL 认证机制（默认 PLAIN）
     * @param saslUsername     SASL 用户名（当使用 SASL 认证时必填）
     * @param saslPassword     SASL 密码（当使用 SASL 认证时必填）
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

    // Getters 保持与record相同的访问方式
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
        // 原有校验逻辑保持不变...
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

    // 保持原有的静态工厂方法
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

    // 重写 equals/hashCode/toString 保持record类似的行为
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
