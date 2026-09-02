package unit.kafka.server.streamaspect

import kafka.network.RequestChannel
import kafka.server.streamaspect.ElasticControllerApis
import kafka.server.{ControllerApisTest, KafkaConfig, RequestLocal, SimpleApiVersionManager}
import org.apache.kafka.common.memory.MemoryPool
import org.apache.kafka.common.message.ApiMessageType.ListenerType
import org.apache.kafka.common.message.{CreateStreamsRequestData, DescribeLicenseRequestData, DescribeStreamsRequestData, ExportClusterManifestRequestData, GetNextNodeIdRequestData, UpdateLicenseRequestData, UpdateLicenseResponseData}
import org.apache.kafka.common.network.{ClientInformation, ListenerName}
import org.apache.kafka.common.protocol.{ApiKeys, Errors, MessageUtil}
import org.apache.kafka.common.requests.s3.{CreateStreamsRequest, CreateStreamsResponse, DescribeLicenseRequest, DescribeLicenseResponse, DescribeStreamsRequest, DescribeStreamsResponse, ExportClusterManifestRequest, ExportClusterManifestResponse, GetNextNodeIdRequest, GetNextNodeIdResponse, UpdateLicenseRequest, UpdateLicenseResponse}
import org.apache.kafka.common.requests.{AbstractRequest, AbstractResponse, RequestContext, RequestHeader}
import org.apache.kafka.common.resource.Resource
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
import org.mockito.Mockito.{mock, never, verify, when}

import java.net.InetAddress
import java.util
import java.util.Properties
import java.util.concurrent.CompletableFuture
import scala.jdk.CollectionConverters.CollectionHasAsScala

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

  @Test
  def shouldRejectCreateStreamsWhenClusterActionIsDenied(): Unit = {
    // Given a stream mutation API, when ClusterAction is denied, then the controller handler is not invoked.
    val controller = mock(classOf[Controller])
    val authorizer = createDenyAllAuthorizer()
    controllerApis = createControllerApis(Some(authorizer), controller)
    val request = buildRequest(new CreateStreamsRequest.Builder(new CreateStreamsRequestData()
      .setNodeId(1)
      .setNodeEpoch(2L)).build(ApiKeys.CREATE_STREAMS.latestVersion))

    controllerApis.handle(request, RequestLocal.NoCaching)

    val response = captureResponse[CreateStreamsResponse](request)
    assertEquals(Errors.CLUSTER_AUTHORIZATION_FAILED.code, response.data.errorCode)
    verify(controller, never()).createStreams(
      ArgumentMatchers.any(classOf[ControllerRequestContext]),
      ArgumentMatchers.any(classOf[CreateStreamsRequestData]))
    assertSingleClusterAuthorization(authorizer, org.apache.kafka.common.acl.AclOperation.CLUSTER_ACTION)
  }

  @Test
  def shouldRejectUpdateLicenseWhenAlterConfigsIsDenied(): Unit = {
    // Given a license mutation API, when AlterConfigs is denied, then the extension handler is not invoked.
    val controller = mock(classOf[Controller])
    val authorizer = createDenyAllAuthorizer()
    controllerApis = createControllerApis(Some(authorizer), controller)
    val request = buildRequest(new UpdateLicenseRequest.Builder(new UpdateLicenseRequestData()).build(ApiKeys.UPDATE_LICENSE.latestVersion))

    controllerApis.handle(request, RequestLocal.NoCaching)

    val response = captureResponse[UpdateLicenseResponse](request)
    assertEquals(Errors.CLUSTER_AUTHORIZATION_FAILED.code, response.data.errorCode)
    verify(controller, never()).handleExtensionRequest(
      ArgumentMatchers.any(classOf[ControllerRequestContext]),
      ArgumentMatchers.eq(ApiKeys.UPDATE_LICENSE),
      ArgumentMatchers.any())
    assertSingleClusterAuthorization(authorizer, org.apache.kafka.common.acl.AclOperation.ALTER_CONFIGS)
  }

  @Test
  def shouldRejectDescribeLicenseWhenDescribeConfigsIsDenied(): Unit = {
    // Given a license describe API, when DescribeConfigs is denied, then the extension handler is not invoked.
    val controller = mock(classOf[Controller])
    val authorizer = createDenyAllAuthorizer()
    controllerApis = createControllerApis(Some(authorizer), controller)
    val request = buildRequest(new DescribeLicenseRequest.Builder(new DescribeLicenseRequestData()).build(ApiKeys.DESCRIBE_LICENSE.latestVersion))

    controllerApis.handle(request, RequestLocal.NoCaching)

    val response = captureResponse[DescribeLicenseResponse](request)
    assertEquals(Errors.CLUSTER_AUTHORIZATION_FAILED.code, response.data.errorCode)
    verify(controller, never()).handleExtensionRequest(
      ArgumentMatchers.any(classOf[ControllerRequestContext]),
      ArgumentMatchers.eq(ApiKeys.DESCRIBE_LICENSE),
      ArgumentMatchers.any())
    assertSingleClusterAuthorization(authorizer, org.apache.kafka.common.acl.AclOperation.DESCRIBE_CONFIGS)
  }

  @Test
  def shouldRejectExportClusterManifestWhenDescribeConfigsIsDenied(): Unit = {
    // Given a cluster manifest export API, when DescribeConfigs is denied, then the extension handler is not invoked.
    val controller = mock(classOf[Controller])
    val authorizer = createDenyAllAuthorizer()
    controllerApis = createControllerApis(Some(authorizer), controller)
    val request = buildRequest(new ExportClusterManifestRequest.Builder(
      new ExportClusterManifestRequestData()).build(ApiKeys.EXPORT_CLUSTER_MANIFEST.latestVersion))

    controllerApis.handle(request, RequestLocal.NoCaching)

    val response = captureResponse[ExportClusterManifestResponse](request)
    assertEquals(Errors.CLUSTER_AUTHORIZATION_FAILED.code, response.data.errorCode)
    verify(controller, never()).handleExtensionRequest(
      ArgumentMatchers.any(classOf[ControllerRequestContext]),
      ArgumentMatchers.eq(ApiKeys.EXPORT_CLUSTER_MANIFEST),
      ArgumentMatchers.any())
    assertSingleClusterAuthorization(authorizer, org.apache.kafka.common.acl.AclOperation.DESCRIBE_CONFIGS)
  }

  @Test
  def shouldRejectDescribeStreamsWhenClusterActionIsDenied(): Unit = {
    // Given a stream describe API, when ClusterAction is denied, then a valid authorization error is returned.
    val controller = mock(classOf[Controller])
    val authorizer = createDenyAllAuthorizer()
    controllerApis = createControllerApis(Some(authorizer), controller)
    val request = buildRequest(new DescribeStreamsRequest.Builder(new DescribeStreamsRequestData()).build(ApiKeys.DESCRIBE_STREAMS.latestVersion))

    controllerApis.handle(request, RequestLocal.NoCaching)

    val response = captureResponse[DescribeStreamsResponse](request)
    assertEquals(Errors.CLUSTER_AUTHORIZATION_FAILED.code, response.data.errorCode)
    verify(controller, never()).describeStreams(
      ArgumentMatchers.any(classOf[ControllerRequestContext]),
      ArgumentMatchers.any(classOf[DescribeStreamsRequestData]))
    assertSingleClusterAuthorization(authorizer, org.apache.kafka.common.acl.AclOperation.CLUSTER_ACTION)
  }

  @Test
  def shouldRejectGetNextNodeIdWithSingleClusterActionAuthorization(): Unit = {
    // Given GetNextNodeId is authorized by the unified extension gate, when denied, then the controller is not invoked.
    val controller = mock(classOf[Controller])
    val authorizer = createDenyAllAuthorizer()
    when(controller.getNextNodeId(
      ArgumentMatchers.any(classOf[ControllerRequestContext]),
      ArgumentMatchers.any(classOf[GetNextNodeIdRequestData])))
      .thenReturn(CompletableFuture.completedFuture(1))
    controllerApis = createControllerApis(Some(authorizer), controller)
    val request = buildRequest(new GetNextNodeIdRequest.Builder(new GetNextNodeIdRequestData())
      .build(ApiKeys.GET_NEXT_NODE_ID.latestVersion))

    controllerApis.handle(request, RequestLocal.NoCaching)

    val response = captureResponse[GetNextNodeIdResponse](request)
    assertEquals(Errors.CLUSTER_AUTHORIZATION_FAILED.code, response.data.errorCode)
    verify(controller, never()).getNextNodeId(
      ArgumentMatchers.any(classOf[ControllerRequestContext]),
      ArgumentMatchers.any(classOf[GetNextNodeIdRequestData]))
    assertSingleClusterAuthorization(authorizer, org.apache.kafka.common.acl.AclOperation.CLUSTER_ACTION)
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

  private def assertSingleClusterAuthorization(
    authorizer: Authorizer,
    operation: org.apache.kafka.common.acl.AclOperation
  ): Unit = {
    val capturedActions: ArgumentCaptor[util.List[org.apache.kafka.server.authorizer.Action]] =
      ArgumentCaptor.forClass(classOf[util.List[org.apache.kafka.server.authorizer.Action]])
    verify(authorizer).authorize(
      ArgumentMatchers.any(),
      capturedActions.capture())
    val actions = capturedActions.getValue.asScala
    assertEquals(1, actions.size)
    assertEquals(operation, actions.head.operation)
    assertEquals(org.apache.kafka.common.resource.ResourceType.CLUSTER, actions.head.resourcePattern.resourceType)
    assertEquals(Resource.CLUSTER_NAME, actions.head.resourcePattern.name)
  }
}
