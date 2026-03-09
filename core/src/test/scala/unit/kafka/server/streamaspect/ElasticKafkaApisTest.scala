package unit.kafka.server.streamaspect

import com.google.common.util.concurrent.MoreExecutors
import kafka.network.RequestChannel
import kafka.server._
import kafka.server.metadata.{ConfigRepository, KRaftMetadataCache, MockConfigRepository, ZkMetadataCache}
import kafka.server.streamaspect.extension.{BrokerExtensionHandleDispatcher, BrokerHandleOps}
import kafka.server.streamaspect.{ElasticKafkaApis, ElasticReplicaManager}
import kafka.utils.TestUtils
import org.apache.kafka.common.memory.MemoryPool
import org.apache.kafka.common.message.ApiMessageType.ListenerType
import org.apache.kafka.common.message.{FetchRequestData, UpdateLicenseRequestData}
import org.apache.kafka.common.network.{ClientInformation, ListenerName}
import org.apache.kafka.common.protocol.{ApiKeys, Errors, MessageUtil}
import org.apache.kafka.common.requests.s3.UpdateLicenseRequest
import org.apache.kafka.common.requests.{AbstractRequest, AbstractResponse, FetchRequest, FetchResponse, RequestContext, RequestHeader}
import org.apache.kafka.common.security.auth.{KafkaPrincipal, SecurityProtocol}
import org.apache.kafka.raft.QuorumConfig
import org.apache.kafka.server.authorizer.Authorizer
import org.apache.kafka.server.common.{FinalizedFeatures, MetadataVersion}
import org.apache.kafka.server.config.KRaftConfigs
import org.junit.jupiter.api.Assertions.{assertEquals, assertNotNull, assertTrue}
import org.junit.jupiter.api.{Tag, Test, Timeout}
import org.mockito.{ArgumentCaptor, ArgumentMatchers}
import org.mockito.Mockito.{mock, never, verify}

import java.net.InetAddress
import java.util.{Collections, Optional}
import scala.collection.Map
import scala.jdk.CollectionConverters.SetHasAsScala

@Timeout(60)
@Tag("S3Unit")
class ElasticKafkaApisTest extends KafkaApisTest {
  override protected val replicaManager: ElasticReplicaManager = mock(classOf[ElasticReplicaManager])
  private var extensionApiMatcherOverride: Option[ApiKeys => Boolean] = None
  private var brokerDispatcherFactoryOverride: Option[BrokerHandleOps => BrokerExtensionHandleDispatcher] = None

  @Test
  def shouldDelegateExtensionApiToBrokerDispatcher(): Unit = {
    var handledByDispatcher = false
    withExtensionHooks(
      _ == ApiKeys.FETCH,
      _ => (_: RequestChannel.Request, _: RequestLocal) => {
        handledByDispatcher = true
        BrokerExtensionHandleDispatcher.Handled
      }) {
      kafkaApis = createKafkaApis()
      val request = buildRequest(new FetchRequest(new FetchRequestData(), ApiKeys.FETCH.latestVersion))
      kafkaApis.handle(request, RequestLocal.NoCaching)

      assertTrue(handledByDispatcher)
      verify(requestChannel, never()).sendResponse(
        ArgumentMatchers.eq(request),
        ArgumentMatchers.any(),
        ArgumentMatchers.any())
    }
  }

  @Test
  def shouldReturnUnsupportedWhenExtensionApiIsNotHandled(): Unit = {
    withExtensionHooks(
      _ == ApiKeys.FETCH,
      _ => (_: RequestChannel.Request, _: RequestLocal) => BrokerExtensionHandleDispatcher.NotHandled) {
      kafkaApis = createKafkaApis()
      val request = buildRequest(new FetchRequest(new FetchRequestData(), ApiKeys.FETCH.latestVersion))
      kafkaApis.handle(request, RequestLocal.NoCaching)

      val response = captureResponse[FetchResponse](request)
      assertEquals(Errors.UNKNOWN_SERVER_ERROR.code, response.data.errorCode)
      assertNotNull(response)
    }
  }

  @Test
  def shouldKeepExplicitUpdateLicenseRouteAheadOfDispatcher(): Unit = {
    var handledByDispatcher = false
    withExtensionHooks(
      _ => true,
      _ => (_: RequestChannel.Request, _: RequestLocal) => {
        handledByDispatcher = true
        BrokerExtensionHandleDispatcher.Handled
      }) {
      kafkaApis = createKafkaApis()
      val request = buildRequest(new UpdateLicenseRequest.Builder(new UpdateLicenseRequestData()).build(ApiKeys.UPDATE_LICENSE.latestVersion))
      kafkaApis.handle(request, RequestLocal.NoCaching)

      assertTrue(!handledByDispatcher)
      verify(requestChannel).sendResponse(
        ArgumentMatchers.eq(request),
        ArgumentMatchers.any(),
        ArgumentMatchers.any())
    }
  }

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
      MoreExecutors.newDirectExecutorService()) {
      override protected def isExtensionApi(apiKey: ApiKeys): Boolean =
        extensionApiMatcherOverride.map(_(apiKey)).getOrElse(super.isExtensionApi(apiKey))

      override protected def createBrokerExtensionHandleDispatcher(ops: BrokerHandleOps): BrokerExtensionHandleDispatcher =
        brokerDispatcherFactoryOverride.map(_(ops)).getOrElse(super.createBrokerExtensionHandleDispatcher(ops))
    }
  }

  private def withExtensionHooks[T](
    extensionApiMatcher: ApiKeys => Boolean,
    dispatcherFactory: BrokerHandleOps => BrokerExtensionHandleDispatcher
  )(block: => T): T = {
    extensionApiMatcherOverride = Some(extensionApiMatcher)
    brokerDispatcherFactoryOverride = Some(dispatcherFactory)
    try block
    finally {
      extensionApiMatcherOverride = None
      brokerDispatcherFactoryOverride = None
    }
  }

  private def buildRequest(
    request: AbstractRequest,
    listenerName: ListenerName = ListenerName.forSecurityProtocol(SecurityProtocol.PLAINTEXT)
  ): RequestChannel.Request = {
    val buffer = request.serializeWithHeader(new RequestHeader(request.apiKey, request.version, clientId, 0))
    val header = RequestHeader.parse(buffer)
    val context = new RequestContext(
      header,
      "1",
      InetAddress.getLocalHost,
      Optional.empty(),
      new KafkaPrincipal(KafkaPrincipal.USER_TYPE, "Alice"),
      listenerName,
      SecurityProtocol.SSL,
      ClientInformation.EMPTY,
      false,
      Optional.of(kafkaPrincipalSerde)
    )
    new RequestChannel.Request(
      processor = 1,
      context = context,
      startTimeNanos = 0,
      MemoryPool.NONE,
      buffer,
      requestChannelMetrics,
      envelope = None
    )
  }

  private def captureResponse[T <: AbstractResponse](request: RequestChannel.Request): T = {
    val capturedResponse: ArgumentCaptor[AbstractResponse] = ArgumentCaptor.forClass(classOf[AbstractResponse])
    verify(requestChannel).sendResponse(
      ArgumentMatchers.eq(request),
      capturedResponse.capture(),
      ArgumentMatchers.any()
    )
    val response = capturedResponse.getValue
    val buffer = MessageUtil.toByteBuffer(
      response.data,
      request.context.header.apiVersion
    )
    AbstractResponse.parseResponse(
      request.context.header.apiKey,
      buffer,
      request.context.header.apiVersion,
    ).asInstanceOf[T]
  }
}
