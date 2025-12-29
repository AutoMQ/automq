package kafka.server.streamaspect

import kafka.network.RequestChannel
import kafka.raft.RaftManager
import kafka.server.QuotaFactory.QuotaManagers
import kafka.server.metadata.KRaftMetadataCache
import kafka.server.{ApiVersionManager, ControllerApis, KafkaConfig, RequestLocal}
import org.apache.kafka.common.acl.AclOperation.CLUSTER_ACTION
import org.apache.kafka.common.errors.ApiException
import org.apache.kafka.common.internals.FatalExitError
import org.apache.kafka.common.message.{DeleteKVsResponseData, DescribeLicenseResponseData, ExportClusterManifestResponseData, GetKVsResponseData, GetNextNodeIdResponseData, PutKVsResponseData, UpdateLicenseResponseData}
import org.apache.kafka.common.protocol.Errors.{NONE, UNKNOWN_SERVER_ERROR}
import org.apache.kafka.common.protocol.{ApiKeys, Errors}
import org.apache.kafka.common.requests.s3._
import org.apache.kafka.common.utils.Time
import org.apache.kafka.controller.{Controller, ControllerRequestContext}
import org.apache.kafka.image.publisher.ControllerRegistrationsPublisher
import org.apache.kafka.server.authorizer.Authorizer
import org.apache.kafka.server.common.ApiMessageAndVersion

import java.util.OptionalLong
import java.util.concurrent.CompletableFuture

class ElasticControllerApis(
  requestChannel: RequestChannel,
  authorizer: Option[Authorizer],
  quotas: QuotaManagers,
  time: Time,
  controller: Controller,
  raftManager: RaftManager[ApiMessageAndVersion],
  config: KafkaConfig,
  clusterId: String,
  registrationsPublisher: ControllerRegistrationsPublisher,
  apiVersionManager: ApiVersionManager,
  metadataCache: KRaftMetadataCache
) extends ControllerApis(requestChannel, authorizer, quotas, time, controller, raftManager, config, clusterId,
  registrationsPublisher, apiVersionManager, metadataCache) {

  def handleExtensionRequest(request: RequestChannel.Request, requestLocal: RequestLocal): Unit = {
    try {
      val handlerFuture: CompletableFuture[Unit] = request.header.apiKey match {
        case ApiKeys.CREATE_STREAMS => handleCreateStream(request)
        case ApiKeys.OPEN_STREAMS => handleOpenStream(request)
        case ApiKeys.CLOSE_STREAMS => handleCloseStream(request)
        case ApiKeys.DELETE_STREAMS => handleDeleteStream(request)
        case ApiKeys.TRIM_STREAMS => handleTrimStream(request)
        case ApiKeys.PREPARE_S3_OBJECT => handlePrepareS3Object(request)
        case ApiKeys.COMMIT_STREAM_SET_OBJECT => handleCommitStreamSetObject(request)
        case ApiKeys.COMMIT_STREAM_OBJECT => handleCommitStreamObject(request)
        case ApiKeys.GET_OPENING_STREAMS => handleGetOpeningStreams(request)
        case ApiKeys.GET_KVS => handleGetKV(request)
        case ApiKeys.PUT_KVS => handlePutKV(request)
        case ApiKeys.DELETE_KVS => handleDeleteKV(request)
        case ApiKeys.UPDATE_LICENSE => handleUpdateLicense(request)
        case ApiKeys.DESCRIBE_LICENSE => handleDescribeLicense(request)
        case ApiKeys.EXPORT_CLUSTER_MANIFEST => handleExportClusterManifest(request)
        case ApiKeys.AUTOMQ_REGISTER_NODE => handleRegisterNode(request)
        case ApiKeys.AUTOMQ_GET_NODES => handleGetNodes(request)
        case ApiKeys.GET_NEXT_NODE_ID => handleGetNextNodeId(request)
        case ApiKeys.DESCRIBE_STREAMS => handleDescribeStreams(request)
        case _ => throw new ApiException(s"Unsupported ApiKey ${request.context.header.apiKey}")
      }

      // This catches exceptions in the future and subsequent completion stages returned by the request handlers.
      handlerFuture.whenComplete { (_, exception) =>
        if (exception != null) {
          // CompletionException does not include the stack frames in its "cause" exception, so we need to
          // log the original exception here
          error(s"Unexpected error handling request ${request.requestDesc(true)} " +
            s"with context ${request.context}", exception)
          requestHelper.handleError(request, exception)
        }
      }
    } catch {
      case e: FatalExitError => throw e
      case t: Throwable => {
        // This catches exceptions in the blocking parts of the request handlers
        error(s"Unexpected error handling request ${request.requestDesc(true)} " +
          s"with context ${request.context}", t)
        requestHelper.handleError(request, t)
      }
    } finally {
      // Only record local completion time if it is unset.
      if (request.apiLocalCompleteTimeNanos < 0) {
        request.apiLocalCompleteTimeNanos = time.nanoseconds
      }
    }

  }

  override def handle(request: RequestChannel.Request, requestLocal: RequestLocal): Unit = {
    request.header.apiKey match {
      case ApiKeys.CREATE_STREAMS
           | ApiKeys.OPEN_STREAMS
           | ApiKeys.CLOSE_STREAMS
           | ApiKeys.DELETE_STREAMS
           | ApiKeys.TRIM_STREAMS
           | ApiKeys.PREPARE_S3_OBJECT
           | ApiKeys.COMMIT_STREAM_SET_OBJECT
           | ApiKeys.COMMIT_STREAM_OBJECT
           | ApiKeys.GET_OPENING_STREAMS
           | ApiKeys.GET_KVS
           | ApiKeys.PUT_KVS
           | ApiKeys.DELETE_KVS
           | ApiKeys.UPDATE_LICENSE
           | ApiKeys.DESCRIBE_LICENSE
           | ApiKeys.EXPORT_CLUSTER_MANIFEST
           | ApiKeys.AUTOMQ_REGISTER_NODE
           | ApiKeys.AUTOMQ_GET_NODES
           | ApiKeys.GET_NEXT_NODE_ID
           | ApiKeys.DESCRIBE_STREAMS
      => handleExtensionRequest(request, requestLocal)
      case _ => super.handle(request, requestLocal)
    }
  }

  private def handleGetNextNodeId(request: RequestChannel.Request): CompletableFuture[Unit] = {
    val getNextNodeIdRequest = request.body[GetNextNodeIdRequest]
    authHelper.authorizeClusterOperation(request, CLUSTER_ACTION)
    val context = new ControllerRequestContext(request.context.header.data, request.context.principal,
      OptionalLong.empty())
    controller.getNextNodeId(context, getNextNodeIdRequest.data()).handle[Unit] { (reply, e) =>
      def createResponseCallback(requestThrottleMs: Int,
        reply: Int,
        e: Throwable): GetNextNodeIdResponse = {
        if (e != null) {
          new GetNextNodeIdResponse(new GetNextNodeIdResponseData().
            setThrottleTimeMs(requestThrottleMs).
            setErrorCode(Errors.forException(e).code))
        } else {
          new GetNextNodeIdResponse(new GetNextNodeIdResponseData().
            setThrottleTimeMs(requestThrottleMs).
            setErrorCode(NONE.code).
            setNodeId(reply))
        }
      }

      requestHelper.sendResponseMaybeThrottle(request,
        requestThrottleMs => createResponseCallback(requestThrottleMs, reply, e))
    }
  }

  // TODO: add acl auth for stream related operations
  def handleCreateStream(request: RequestChannel.Request): CompletableFuture[Unit] = {
    val createStreamRequest = request.body[CreateStreamsRequest]
    val context = new ControllerRequestContext(request.context.header.data, request.context.principal,
      OptionalLong.empty())
    controller.createStreams(context, createStreamRequest.data)
      .handle[Unit] { (result, exception) =>
        if (exception != null) {
          requestHelper.handleError(request, exception)
        } else {
          requestHelper.sendResponseMaybeThrottle(request, requestThrottleMs => {
            new CreateStreamsResponse(result.setThrottleTimeMs(requestThrottleMs))
          })
        }
      }
  }

  def handleOpenStream(request: RequestChannel.Request): CompletableFuture[Unit] = {
    val openStreamRequest = request.body[OpenStreamsRequest]
    val context = new ControllerRequestContext(request.context.header.data, request.context.principal,
      OptionalLong.empty())
    controller.openStreams(context, openStreamRequest.data)
      .handle[Unit] { (result, exception) =>
        if (exception != null) {
          requestHelper.handleError(request, exception)
        } else {
          requestHelper.sendResponseMaybeThrottle(request, requestThrottleMs => {
            new OpenStreamsResponse(result.setThrottleTimeMs(requestThrottleMs))
          })
        }
      }
  }

  def handleCloseStream(request: RequestChannel.Request): CompletableFuture[Unit] = {
    val closeStreamRequest = request.body[CloseStreamsRequest]
    val context = new ControllerRequestContext(request.context.header.data, request.context.principal,
      OptionalLong.empty())
    controller.closeStreams(context, closeStreamRequest.data)
      .handle[Unit] { (result, exception) =>
        if (exception != null) {
          requestHelper.handleError(request, exception)
        } else {
          requestHelper.sendResponseMaybeThrottle(request, requestThrottleMs => {
            new CloseStreamsResponse(result.setThrottleTimeMs(requestThrottleMs))
          })
        }
      }
  }

  def handleDeleteStream(request: RequestChannel.Request): CompletableFuture[Unit] = {
    val deleteStreamRequest = request.body[DeleteStreamsRequest]
    val context = new ControllerRequestContext(request.context.header.data, request.context.principal,
      OptionalLong.empty())
    controller.deleteStreams(context, deleteStreamRequest.data)
      .handle[Unit] { (result, exception) =>
        if (exception != null) {
          requestHelper.handleError(request, exception)
        } else {
          requestHelper.sendResponseMaybeThrottle(request, requestThrottleMs => {
            new DeleteStreamsResponse(result.setThrottleTimeMs(requestThrottleMs))
          })
        }
      }
  }

  def handleTrimStream(request: RequestChannel.Request): CompletableFuture[Unit] = {
    val trimStreamRequest = request.body[TrimStreamsRequest]
    val context = new ControllerRequestContext(request.context.header.data, request.context.principal,
      OptionalLong.empty())
    controller.trimStreams(context, trimStreamRequest.data)
      .handle[Unit] { (result, exception) =>
        if (exception != null) {
          requestHelper.handleError(request, exception)
        } else {
          requestHelper.sendResponseMaybeThrottle(request, requestThrottleMs => {
            new TrimStreamsResponse(result.setThrottleTimeMs(requestThrottleMs))
          })
        }
      }
  }

  def handlePrepareS3Object(request: RequestChannel.Request): CompletableFuture[Unit] = {
    val prepareS3ObjectRequest = request.body[PrepareS3ObjectRequest]
    val context = new ControllerRequestContext(request.context.header.data, request.context.principal,
      OptionalLong.empty())
    controller.prepareObject(context, prepareS3ObjectRequest.data)
      .handle[Unit] { (result, exception) =>
        if (exception != null) {
          requestHelper.handleError(request, exception)
        } else {
          requestHelper.sendResponseMaybeThrottle(request, requestThrottleMs => {
            new PrepareS3ObjectResponse(result.setThrottleTimeMs(requestThrottleMs))
          })
        }
      }
  }

  def handleCommitStreamSetObject(request: RequestChannel.Request): CompletableFuture[Unit] = {
    val commitStreamSetObjectRequest = request.body[CommitStreamSetObjectRequest]
    val context = new ControllerRequestContext(request.context.header.data, request.context.principal,
      OptionalLong.empty())
    controller.commitStreamSetObject(context, commitStreamSetObjectRequest.data)
      .handle[Unit] { (result, exception) =>
        if (exception != null) {
          requestHelper.handleError(request, exception)
        } else {
          requestHelper.sendResponseMaybeThrottle(request, requestThrottleMs => {
            new CommitStreamSetObjectResponse(result.setThrottleTimeMs(requestThrottleMs))
          })
        }
      }
  }

  def handleCommitStreamObject(request: RequestChannel.Request): CompletableFuture[Unit] = {
    val commitStreamObjectRequest = request.body[CommitStreamObjectRequest]
    val context = new ControllerRequestContext(request.context.header.data, request.context.principal,
      OptionalLong.empty())
    controller.commitStreamObject(context, commitStreamObjectRequest.data)
      .handle[Unit] { (result, exception) =>
        if (exception != null) {
          requestHelper.handleError(request, exception)
        } else {
          requestHelper.sendResponseMaybeThrottle(request, requestThrottleMs => {
            new CommitStreamObjectResponse(result.setThrottleTimeMs(requestThrottleMs))
          })
        }
      }
  }

  def handleGetOpeningStreams(request: RequestChannel.Request): CompletableFuture[Unit] = {
    val getOpeningStreamsRequest = request.body[GetOpeningStreamsRequest]
    val context = new ControllerRequestContext(request.context.header.data, request.context.principal,
      OptionalLong.empty())
    controller.getOpeningStreams(context, getOpeningStreamsRequest.data)
      .handle[Unit] { (result, exception) =>
        if (exception != null) {
          requestHelper.handleError(request, exception)
        } else {
          requestHelper.sendResponseMaybeThrottle(request, requestThrottleMs => {
            new GetOpeningStreamsResponse(result.setThrottleTimeMs(requestThrottleMs))
          })
        }
      }
  }

  def handleDescribeStreams(request: RequestChannel.Request): CompletableFuture[Unit] = {
    val getDescribeStreamsRequest = request.body[DescribeStreamsRequest]
    val context = new ControllerRequestContext(request.context.header.data, request.context.principal,
      OptionalLong.empty())
    controller.describeStreams(context, getDescribeStreamsRequest.data)
      .handle[Unit] { (result, exception) =>
        if (exception != null) {
          requestHelper.handleError(request, exception)
        } else {
          requestHelper.sendResponseMaybeThrottle(request, requestThrottleMs => {
            new DescribeStreamsResponse(result.setThrottleTimeMs(requestThrottleMs))
          })
        }
      }
  }

  def handleGetKV(request: RequestChannel.Request): CompletableFuture[Unit] = {
    val getKVRequest = request.body[GetKVsRequest]
    val context = new ControllerRequestContext(request.context.header.data, request.context.principal,
      OptionalLong.empty())
    controller.getKVs(context, getKVRequest.data)
      .handle[Unit] { (result, exception) =>
        if (exception != null) {
          requestHelper.handleError(request, exception)
        } else if (result == null) {
          requestHelper.sendResponseMaybeThrottle(request, requestThrottleMs => {
            new GetKVsResponse(new GetKVsResponseData().
              setThrottleTimeMs(requestThrottleMs).
              setErrorCode(UNKNOWN_SERVER_ERROR.code))
          })
        } else {
          requestHelper.sendResponseMaybeThrottle(request, requestThrottleMs => {
            new GetKVsResponse(result.setThrottleTimeMs(requestThrottleMs))
          })
        }
      }
  }

  def handlePutKV(request: RequestChannel.Request): CompletableFuture[Unit] = {
    val putKVRequest = request.body[PutKVsRequest]
    val context = new ControllerRequestContext(request.context.header.data, request.context.principal,
      OptionalLong.empty())
    controller.putKVs(context, putKVRequest.data)
      .handle[Unit] { (result, exception) =>
        if (exception != null) {
          requestHelper.handleError(request, exception)
        } else if (result == null) {
          requestHelper.sendResponseMaybeThrottle(request, requestThrottleMs => {
            new PutKVsResponse(new PutKVsResponseData().
              setThrottleTimeMs(requestThrottleMs).
              setErrorCode(UNKNOWN_SERVER_ERROR.code))
          })
        } else {
          requestHelper.sendResponseMaybeThrottle(request, requestThrottleMs => {
            new PutKVsResponse(result.setThrottleTimeMs(requestThrottleMs))
          })
        }
      }
  }

  def handleDeleteKV(request: RequestChannel.Request): CompletableFuture[Unit] = {
    val deleteKVRequest = request.body[DeleteKVsRequest]
    val context = new ControllerRequestContext(request.context.header.data, request.context.principal,
      OptionalLong.empty())
    controller.deleteKVs(context, deleteKVRequest.data)
      .handle[Unit] { (result, exception) =>
        if (exception != null) {
          requestHelper.handleError(request, exception)
        } else if (result == null) {
          requestHelper.sendResponseMaybeThrottle(request, requestThrottleMs => {
            new DeleteKVsResponse(new DeleteKVsResponseData().
              setThrottleTimeMs(requestThrottleMs).
              setErrorCode(UNKNOWN_SERVER_ERROR.code))
          })
        } else {
          requestHelper.sendResponseMaybeThrottle(request, requestThrottleMs => {
            new DeleteKVsResponse(result.setThrottleTimeMs(requestThrottleMs))
          })
        }
      }
  }

  def handleUpdateLicense(request: RequestChannel.Request): CompletableFuture[Unit] = {
    val updateLicenseRequest = request.body[UpdateLicenseRequest]
    val context = new ControllerRequestContext(request.context.header.data, request.context.principal,
      OptionalLong.empty())
    controller.updateLicense(context, updateLicenseRequest.data)
      .handle[Unit] { (result, exception) =>
        if (exception != null) {
          requestHelper.handleError(request, exception)
        } else if (result == null) {
          requestHelper.sendResponseMaybeThrottle(request, requestThrottleMs => {
            new UpdateLicenseResponse(new UpdateLicenseResponseData().
              setThrottleTimeMs(requestThrottleMs).
              setErrorCode(UNKNOWN_SERVER_ERROR.code))
          })
        } else {
          requestHelper.sendResponseMaybeThrottle(request, requestThrottleMs => {
            new UpdateLicenseResponse(result.setThrottleTimeMs(requestThrottleMs))
          })
        }
      }
  }

  def handleDescribeLicense(request: RequestChannel.Request): CompletableFuture[Unit] = {
    val describeLicenseRequest = request.body[DescribeLicenseRequest]
    val context = new ControllerRequestContext(request.context.header.data, request.context.principal,
      OptionalLong.empty())
    controller.describeLicense(context, describeLicenseRequest.data)
      .handle[Unit] { (result, exception) =>
        if (exception != null) {
          requestHelper.handleError(request, exception)
        } else if (result == null) {
          requestHelper.sendResponseMaybeThrottle(request, requestThrottleMs => {
            new DescribeLicenseResponse(new DescribeLicenseResponseData().
              setThrottleTimeMs(requestThrottleMs).
              setErrorCode(UNKNOWN_SERVER_ERROR.code).
              setErrorMessage(UNKNOWN_SERVER_ERROR.message))
          })
        } else {
          requestHelper.sendResponseMaybeThrottle(request, requestThrottleMs => {
            new DescribeLicenseResponse(result.setThrottleTimeMs(requestThrottleMs))
          })
        }
      }
  }

  def handleExportClusterManifest(request: RequestChannel.Request): CompletableFuture[Unit] = {
    val exportClusterManifestRequest = request.body[ExportClusterManifestRequest]
    val context = new ControllerRequestContext(request.context.header.data, request.context.principal,
      OptionalLong.empty())
    controller.exportClusterManifest(context, exportClusterManifestRequest.data)
      .handle[Unit] { (result, exception) =>
        if (exception != null) {
          requestHelper.handleError(request, exception)
        } else if (result == null) {
          requestHelper.sendResponseMaybeThrottle(request, requestThrottleMs => {
            new ExportClusterManifestResponse(new ExportClusterManifestResponseData().
              setThrottleTimeMs(requestThrottleMs).
              setErrorCode(UNKNOWN_SERVER_ERROR.code))
          })
        } else {
          requestHelper.sendResponseMaybeThrottle(request, requestThrottleMs => {
            new ExportClusterManifestResponse(result.setThrottleTimeMs(requestThrottleMs))
          })
        }
      }
  }

  def handleRegisterNode(request: RequestChannel.Request): CompletableFuture[Unit] = {
    val req = request.body[AutomqRegisterNodeRequest]
    val context = new ControllerRequestContext(request.context.header.data, request.context.principal,
      OptionalLong.empty())
    controller.registerNode(context, req)
      .handle[Unit] { (result, exception) =>
        if (exception != null) {
          requestHelper.handleError(request, exception)
        } else {
          requestHelper.sendResponseMaybeThrottle(request, requestThrottleMs => {
            new AutomqRegisterNodeResponse(result.setThrottleTimeMs(requestThrottleMs))
          })
        }
      }
  }

  def handleGetNodes(request: RequestChannel.Request): CompletableFuture[Unit] = {
    val req = request.body[AutomqGetNodesRequest]
    val context = new ControllerRequestContext(request.context.header.data, request.context.principal,
      OptionalLong.empty())
    controller.getNodes(context, req)
      .handle[Unit] { (result, exception) =>
        if (exception != null) {
          requestHelper.handleError(request, exception)
        } else {
          requestHelper.sendResponseMaybeThrottle(request, requestThrottleMs => {
            new AutomqGetNodesResponse(result.setThrottleTimeMs(requestThrottleMs))
          })
        }
      }
  }
}
