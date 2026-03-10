package unit.kafka.server.streamaspect

import kafka.network.RequestChannel
import kafka.server.streamaspect.ElasticControllerApis
import kafka.server.{ControllerApisTest, KafkaConfig, RequestLocal, SimpleApiVersionManager}
import org.apache.kafka.common.memory.MemoryPool
import org.apache.kafka.common.message.ApiMessageType.ListenerType
import org.apache.kafka.common.message.{UpdateLicenseRequestData, UpdateLicenseResponseData}
import org.apache.kafka.common.network.{ClientInformation, ListenerName}
import org.apache.kafka.common.protocol.{ApiKeys, Errors, MessageUtil}
import org.apache.kafka.common.requests.s3.{UpdateLicenseRequest, UpdateLicenseResponse}
import org.apache.kafka.common.requests.{AbstractRequest, AbstractResponse, RequestContext, RequestHeader}
import org.apache.kafka.common.security.auth.{KafkaPrincipal, SecurityProtocol}
import org.apache.kafka.controller.Controller
import org.apache.kafka.controller.ControllerRequestContext
import org.apache.kafka.image.publisher.ControllerRegistrationsPublisher
import org.apache.kafka.raft.QuorumConfig
import org.apache.kafka.server.authorizer.Authorizer
import org.apache.kafka.server.common.{FinalizedFeatures, MetadataVersion}
import org.apache.kafka.server.config.KRaftConfigs
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.{Tag, Test, Timeout}
import org.mockito.{ArgumentCaptor, ArgumentMatchers}
import org.mockito.Mockito.{mock, verify, when}

import java.net.InetAddress
import java.util.Properties
import java.util.concurrent.CompletableFuture

@Timeout(60)
@Tag("S3Unit")
class ElasticControllerApisTest extends ControllerApisTest {

  @Test
  def shouldDelegateUpdateLicenseToControllerExtension(): Unit = {
    val controller = mock(classOf[Controller])
    val extensionResponse = new UpdateLicenseResponseData()
      .setErrorCode(Errors.NONE.code)
      .setErrorMessage("")
      .setThrottleTimeMs(0)
    when(controller.handleExtensionRequest(
      ArgumentMatchers.any(classOf[ControllerRequestContext]),
      ArgumentMatchers.eq(ApiKeys.UPDATE_LICENSE),
      ArgumentMatchers.any()))
      .thenReturn(CompletableFuture.completedFuture(new UpdateLicenseResponse(extensionResponse)))

    controllerApis = createControllerApis(None, controller)
    val request = buildRequest(new UpdateLicenseRequest.Builder(new UpdateLicenseRequestData()).build(ApiKeys.UPDATE_LICENSE.latestVersion))
    controllerApis.handle(request, RequestLocal.NoCaching)

    val response = captureResponse[UpdateLicenseResponse](request)
    assertEquals(Errors.NONE.code, response.data.errorCode)
    verify(controller).handleExtensionRequest(
      ArgumentMatchers.any(classOf[ControllerRequestContext]),
      ArgumentMatchers.eq(ApiKeys.UPDATE_LICENSE),
      ArgumentMatchers.any())
  }

  @Test
  def shouldMapNullControllerExtensionResponseToUnsupportedVersion(): Unit = {
    val controller = mock(classOf[Controller])
    when(controller.handleExtensionRequest(
      ArgumentMatchers.any(classOf[ControllerRequestContext]),
      ArgumentMatchers.eq(ApiKeys.UPDATE_LICENSE),
      ArgumentMatchers.any()))
      .thenReturn(CompletableFuture.completedFuture(null))

    controllerApis = createControllerApis(None, controller)
    val request = buildRequest(new UpdateLicenseRequest.Builder(new UpdateLicenseRequestData()).build(ApiKeys.UPDATE_LICENSE.latestVersion))
    controllerApis.handle(request, RequestLocal.NoCaching)

    val response = captureResponse[UpdateLicenseResponse](request)
    assertEquals(Errors.UNSUPPORTED_VERSION.code, response.data.errorCode)
  }

  override def createControllerApis(authorizer: Option[Authorizer],
    controller: Controller,
    props: Properties = new Properties(),
    throttle: Boolean = false): ElasticControllerApis = {

    props.put(KRaftConfigs.NODE_ID_CONFIG, nodeId: java.lang.Integer)
    props.put(KRaftConfigs.PROCESS_ROLES_CONFIG, "controller")
    props.put(KRaftConfigs.CONTROLLER_LISTENER_NAMES_CONFIG, "PLAINTEXT")
    props.put(QuorumConfig.QUORUM_VOTERS_CONFIG, s"$nodeId@localhost:9092")
    new ElasticControllerApis(
      requestChannel,
      authorizer,
      if (throttle) quotasAlwaysThrottleControllerMutations else quotasNeverThrottleControllerMutations,
      time,
      controller,
      raftManager,
      new KafkaConfig(props),
      "JgxuGe9URy-E-ceaL04lEw",
      new ControllerRegistrationsPublisher(),
      new SimpleApiVersionManager(
        ListenerType.CONTROLLER,
        true,
        false,
        () => FinalizedFeatures.fromKRaftVersion(MetadataVersion.latestTesting())),
      metadataCache
    )
  }

  private def buildRequest(
    request: AbstractRequest,
    listenerName: ListenerName = ListenerName.forSecurityProtocol(SecurityProtocol.PLAINTEXT)
  ): RequestChannel.Request = {
    val buffer = request.serializeWithHeader(new RequestHeader(request.apiKey, request.version, clientID, 0))
    val header = RequestHeader.parse(buffer)
    val context = new RequestContext(
      header,
      "1",
      InetAddress.getLocalHost,
      KafkaPrincipal.ANONYMOUS,
      listenerName,
      SecurityProtocol.PLAINTEXT,
      ClientInformation.EMPTY,
      false
    )
    new RequestChannel.Request(
      processor = 1,
      context = context,
      startTimeNanos = 0,
      MemoryPool.NONE,
      buffer,
      requestChannelMetrics
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
    val buffer = MessageUtil.toByteBuffer(response.data, request.context.header.apiVersion)
    AbstractResponse.parseResponse(
      request.context.header.apiKey,
      buffer,
      request.context.header.apiVersion,
    ).asInstanceOf[T]
  }
}
