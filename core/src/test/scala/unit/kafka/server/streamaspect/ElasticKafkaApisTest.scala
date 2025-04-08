package unit.kafka.server.streamaspect

import com.google.common.util.concurrent.MoreExecutors
import kafka.server._
import kafka.server.metadata.{ConfigRepository, KRaftMetadataCache, MockConfigRepository, ZkMetadataCache}
import kafka.server.streamaspect.{ElasticKafkaApis, ElasticReplicaManager}
import kafka.utils.TestUtils
import org.apache.kafka.common.message.ApiMessageType.ListenerType
import org.apache.kafka.common.protocol.ApiKeys
import org.apache.kafka.raft.QuorumConfig
import org.apache.kafka.server.authorizer.Authorizer
import org.apache.kafka.server.common.{FinalizedFeatures, MetadataVersion}
import org.apache.kafka.server.config.KRaftConfigs
import org.junit.jupiter.api.{Tag, Timeout}
import org.mockito.Mockito.mock

import java.util.Collections
import scala.collection.Map
import scala.jdk.CollectionConverters.SetHasAsScala

@Timeout(60)
@Tag("S3Unit")
class ElasticKafkaApisTest extends KafkaApisTest {
  override protected val replicaManager: ElasticReplicaManager = mock(classOf[ElasticReplicaManager])

  override def createKafkaApis(interBrokerProtocolVersion: MetadataVersion = MetadataVersion.latestTesting,
    authorizer: Option[Authorizer] = None,
    enableForwarding: Boolean = false,
    configRepository: ConfigRepository = new MockConfigRepository(),
    raftSupport: Boolean = false,
    overrideProperties: Map[String, String] = Map.empty): ElasticKafkaApis = {
    val properties = if (raftSupport) {
      val properties = TestUtils.createBrokerConfig(brokerId, "")
      properties.put(KRaftConfigs.NODE_ID_CONFIG, brokerId.toString)
      properties.put(KRaftConfigs.PROCESS_ROLES_CONFIG, "broker")
      val voterId = brokerId + 1
      properties.put(QuorumConfig.QUORUM_VOTERS_CONFIG, s"$voterId@localhost:9093")
      properties.put(KRaftConfigs.CONTROLLER_LISTENER_NAMES_CONFIG, "SSL")
      properties
    } else {
      TestUtils.createBrokerConfig(brokerId, "zk")
    }
    overrideProperties.foreach(p => properties.put(p._1, p._2))
    TestUtils.setIbpAndMessageFormatVersions(properties, interBrokerProtocolVersion)
    val config = new KafkaConfig(properties)

    val forwardingManagerOpt = if (enableForwarding)
      Some(this.forwardingManager)
    else
      None

    val metadataSupport = if (raftSupport) {
      // it will be up to the test to replace the default ZkMetadataCache implementation
      // with a KRaftMetadataCache instance
      metadataCache match {
        case cache: KRaftMetadataCache => RaftSupport(forwardingManager, cache)
        case _ => throw new IllegalStateException("Test must set an instance of KRaftMetadataCache")
      }
    } else {
      metadataCache match {
        case zkMetadataCache: ZkMetadataCache =>
          ZkSupport(adminManager, controller, zkClient, forwardingManagerOpt, zkMetadataCache, brokerEpochManager)
        case _ => throw new IllegalStateException("Test must set an instance of ZkMetadataCache")
      }
    }

    val listenerType = if (raftSupport) ListenerType.BROKER else ListenerType.ZK_BROKER
    val enabledApis = if (enableForwarding) {
      ApiKeys.apisForListener(listenerType).asScala ++ Set(ApiKeys.ENVELOPE)
    } else {
      ApiKeys.apisForListener(listenerType).asScala.toSet
    }
    val apiVersionManager = new SimpleApiVersionManager(
      listenerType,
      enabledApis,
      BrokerFeatures.defaultSupportedFeatures(true),
      true,
      false,
      () => new FinalizedFeatures(MetadataVersion.latestTesting(), Collections.emptyMap[String, java.lang.Short], 0, raftSupport))

    val clientMetricsManagerOpt = if (raftSupport) Some(clientMetricsManager) else None

    new ElasticKafkaApis(
      requestChannel = requestChannel,
      metadataSupport = metadataSupport,
      replicaManager = replicaManager,
      groupCoordinator = groupCoordinator,
      txnCoordinator = txnCoordinator,
      autoTopicCreationManager = autoTopicCreationManager,
      brokerId = brokerId,
      config = config,
      configRepository = configRepository,
      metadataCache = metadataCache,
      metrics = metrics,
      authorizer = authorizer,
      quotas = quotas,
      fetchManager = fetchManager,
      brokerTopicStats = brokerTopicStats,
      clusterId = clusterId,
      time = time,
      tokenManager = null,
      apiVersionManager = apiVersionManager,
      clientMetricsManager = clientMetricsManagerOpt,
      MoreExecutors.newDirectExecutorService(),
      MoreExecutors.newDirectExecutorService())
  }
}
