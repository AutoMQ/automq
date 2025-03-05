package kafka.automq.tmp;

import kafka.automq.zonerouter.ClientIdMetadata;
import kafka.automq.zonerouter.ClientType;
import kafka.automq.zonerouter.ProduceRouter;
import kafka.server.KafkaConfig;
import kafka.server.MetadataCache;
import kafka.server.RequestLocal;
import kafka.server.streamaspect.ElasticKafkaApis;

import org.apache.kafka.common.Node;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.message.MetadataResponseData;
import org.apache.kafka.common.record.MemoryRecords;
import org.apache.kafka.common.record.RecordValidationStats;
import org.apache.kafka.common.requests.ProduceResponse;
import org.apache.kafka.common.requests.s3.AutomqZoneRouterResponse;
import org.apache.kafka.common.utils.LogContext;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.image.MetadataDelta;
import org.apache.kafka.image.MetadataImage;
import org.apache.kafka.image.loader.LoaderManifest;
import org.apache.kafka.image.publisher.MetadataPublisher;
import org.apache.kafka.metadata.BrokerRegistration;

import com.automq.stream.s3.operator.BucketURI;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.function.Consumer;

// tmp class for test
public class ObjectCrossZoneProduceRouter implements ProduceRouter, MetadataPublisher {
    private static final Logger LOGGER = LoggerFactory.getLogger(ObjectCrossZoneProduceRouter.class);
    private final ElasticKafkaApis kafkaApis;
    private final MetadataCache metadataCache;
    private final AsyncSender asyncSender;
    private KafkaConfig config;

    public ObjectCrossZoneProduceRouter(ElasticKafkaApis kafkaApis, MetadataCache metadataCache,
        KafkaConfig kafkaConfig,
        BucketURI bucketURI) {
        this.kafkaApis = kafkaApis;
        this.metadataCache = metadataCache;
        this.config = kafkaConfig;

        if (kafkaConfig.rack().isEmpty()) {
            throw new IllegalArgumentException("The node rack should be set when enable cross available zone router");
        }
        asyncSender = new AsyncSender.BrokersAsyncSender(kafkaConfig, kafkaApis.metrics(), Time.SYSTEM, "router-client-id", new LogContext());
        LOGGER.info("Start object produce router with config={}", bucketURI);
        if (config.nodeId() == 2) {

        }
    }

    @Override
    public void handleProduceRequest(
        short apiVersion,
        ClientIdMetadata clientId,
        int timeout,
        short requiredAcks,
        boolean internalTopicsAllowed,
        String transactionId,
        Map<TopicPartition, MemoryRecords> entriesPerPartition,
        Consumer<Map<TopicPartition, ProduceResponse.PartitionResponse>> responseCallback,
        Consumer<Map<TopicPartition, RecordValidationStats>> recordValidationStatsCallback,
        RequestLocal requestLocal
    ) {
        kafkaApis.handleProduceAppendJavaCompatible(
            timeout,
            requiredAcks,
            internalTopicsAllowed,
            transactionId,
            entriesPerPartition,
            rst -> {
                responseCallback.accept(rst);
                return null;
            },
            rst -> {
                recordValidationStatsCallback.accept(rst);
                return null;
            },
            apiVersion,
            requestLocal
        );
    }

    @Override
    public CompletableFuture<AutomqZoneRouterResponse> handleZoneRouterRequest(byte[] metadata) {
        return null;
    }

    @Override
    public List<MetadataResponseData.MetadataResponseTopic> handleMetadataResponse(String clientId,
        List<MetadataResponseData.MetadataResponseTopic> topics) {
        ClientIdMetadata clientIdMetadata = ClientIdMetadata.of(clientId);
        String clientRack = clientIdMetadata.rack();
        if (clientRack == null || clientIdMetadata.clientType() != ClientType.CONSUMER || !clientIdMetadata.rack().equals("az2")) {
            return topics;
        }
        topics.forEach(metadataResponseTopic -> {
            metadataResponseTopic.partitions().forEach(metadataResponsePartition -> {
                metadataResponsePartition.setLeaderId(2);
                metadataResponsePartition.setIsrNodes(List.of(2));
                metadataResponsePartition.setReplicaNodes(List.of(2));
            });
        });
        return topics;
    }

    @Override
    public Optional<Node> getLeaderNode(int leaderId, ClientIdMetadata clientId,
        String listenerName) {
        BrokerRegistration target = null;
        if (clientId.clientType() == ClientType.CONSUMER) {
            if (clientId.rack().equals("az2")) {
                target = metadataCache.getNode(2);
            }
        }
        if (target == null) {
            target = metadataCache.getNode(leaderId);
        }
        return target.node(listenerName);
    }

    @Override
    public String name() {
        return "ObjectCrossZoneProduceRouter";
    }

    @Override
    public void onMetadataUpdate(MetadataDelta delta, MetadataImage newImage, LoaderManifest manifest) {
        // TODO
        // node 变更和 partition 变更需要重新刷新，
//        delta.topicsDelta().changedTopic()
        // 两种方式：主动 set 和被动 get
        // metadata update 有点复杂
    }

}
