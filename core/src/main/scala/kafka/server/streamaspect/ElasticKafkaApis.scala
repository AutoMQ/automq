package kafka.server.streamaspect

import com.automq.stream.s3.metrics.TimerUtil
import com.automq.stream.utils.threads.S3StreamThreadPoolMonitor
import com.yammer.metrics.core.Histogram
import kafka.automq.zonerouter.{ClientIdMetadata, NoopProduceRouter, ProduceRouter}
import kafka.coordinator.transaction.TransactionCoordinator
import kafka.log.streamaspect.{ElasticLogManager, ReadHint}
import kafka.metrics.KafkaMetricsUtil
import kafka.network.RequestChannel
import kafka.server.QuotaFactory.QuotaManagers
import kafka.server._
import kafka.server.metadata.ConfigRepository
import kafka.server.streamaspect.ElasticKafkaApis.{LAST_RECORD_TIMESTAMP, PRODUCE_ACK_TIME_HIST, PRODUCE_CALLBACK_TIME_HIST, PRODUCE_TIME_HIST}
import kafka.utils.Implicits.MapExtensionMethods
import org.apache.kafka.admin.AdminUtils
import org.apache.kafka.common.acl.AclOperation.{CLUSTER_ACTION, READ, WRITE}
import org.apache.kafka.common.errors.{ApiException, UnsupportedCompressionTypeException}
import org.apache.kafka.common.internals.FatalExitError
import org.apache.kafka.common.message.{DeleteTopicsRequestData, FetchResponseData, MetadataResponseData}
import org.apache.kafka.common.metrics.Metrics
import org.apache.kafka.common.network.{ListenerName, NetworkSend, Send}
import org.apache.kafka.common.protocol.{ApiKeys, Errors}
import org.apache.kafka.common.record._
import org.apache.kafka.common.replica.ClientMetadata
import org.apache.kafka.common.replica.ClientMetadata.DefaultClientMetadata
import org.apache.kafka.common.requests.ProduceResponse.PartitionResponse
import org.apache.kafka.common.requests.s3.{AutomqGetPartitionSnapshotRequest, AutomqUpdateGroupRequest, AutomqUpdateGroupResponse, AutomqZoneRouterRequest}
import org.apache.kafka.common.requests.{AbstractResponse, DeleteTopicsRequest, DeleteTopicsResponse, FetchRequest, FetchResponse, ProduceRequest, ProduceResponse, RequestUtils}
import org.apache.kafka.common.resource.Resource.CLUSTER_NAME
import org.apache.kafka.common.resource.ResourceType.{CLUSTER, TOPIC, TRANSACTIONAL_ID}
import org.apache.kafka.common.utils.Time
import org.apache.kafka.common.{Node, TopicIdPartition, TopicPartition, Uuid}
import org.apache.kafka.coordinator.group.GroupCoordinator
import org.apache.kafka.server.ClientMetricsManager
import org.apache.kafka.server.authorizer.Authorizer
import org.apache.kafka.server.metrics.KafkaMetricsGroup
import org.apache.kafka.server.record.BrokerCompressionType
import org.apache.kafka.storage.internals.log.{FetchIsolation, FetchParams, FetchPartitionData}

import java.util
import java.util.concurrent.atomic.AtomicLong
import java.util.concurrent.{ExecutorService, TimeUnit}
import java.util.stream.IntStream
import java.util.{Collections, Optional}
import scala.annotation.nowarn
import scala.collection.{Map, Seq, mutable}
import scala.jdk.CollectionConverters.{CollectionHasAsScala, MapHasAsJava, MapHasAsScala, SeqHasAsJava}
import scala.jdk.javaapi.OptionConverters

object ElasticKafkaApis {
  val metricsGroup = new KafkaMetricsGroup(this.getClass)
  val LAST_RECORD_TIMESTAMP = new AtomicLong()
  val PRODUCE_TIME_HIST: Histogram = metricsGroup.newHistogram("ProduceTimeNanos")
  val PRODUCE_CALLBACK_TIME_HIST: Histogram = metricsGroup.newHistogram("ProduceCallbackTimeNanos")
  val PRODUCE_ACK_TIME_HIST: Histogram = metricsGroup.newHistogram("ProduceAckTimeNanos")
}

class ElasticKafkaApis(
  requestChannel: RequestChannel,
  metadataSupport: MetadataSupport,
  replicaManager: ReplicaManager,
  groupCoordinator: GroupCoordinator,
  txnCoordinator: TransactionCoordinator,
  autoTopicCreationManager: AutoTopicCreationManager,
  brokerId: Int,
  config: KafkaConfig,
  configRepository: ConfigRepository,
  metadataCache: MetadataCache,
  metrics: Metrics,
  authorizer: Option[Authorizer],
  quotas: QuotaManagers,
  fetchManager: FetchManager,
  brokerTopicStats: BrokerTopicStats,
  clusterId: String,
  time: Time,
  tokenManager: DelegationTokenManager,
  apiVersionManager: ApiVersionManager,
  clientMetricsManager: Option[ClientMetricsManager],
  val deleteTopicHandleExecutor: ExecutorService = S3StreamThreadPoolMonitor.createAndMonitor(1, 1, 0L, TimeUnit.MILLISECONDS, "kafka-apis-delete-topic-handle-executor", true, 1000),
  val listOffsetHandleExecutor: ExecutorService = S3StreamThreadPoolMonitor.createAndMonitor(1, 1, 0L, TimeUnit.MILLISECONDS, "kafka-apis-list-offset-handle-executor", true, 1000)
) extends KafkaApis(requestChannel, metadataSupport, replicaManager, groupCoordinator, txnCoordinator,
  autoTopicCreationManager, brokerId, config, configRepository, metadataCache, metrics, authorizer, quotas,
  fetchManager, brokerTopicStats, clusterId, time, tokenManager, apiVersionManager, clientMetricsManager) {

  private var produceRouter: ProduceRouter = new NoopProduceRouter(this, metadataCache)

  /**
   * Generate a map of topic -> [(partitionId, epochId)] based on provided topicsRequestData.
   *
   * @param topicsRequestData the requesting DeleteTopicsRequestData
   * @return a map of topic -> [(partitionId, epochId)]
   */
  private def getPartitionEpochSnapshotMap(
    topicsRequestData: DeleteTopicsRequestData): util.HashMap[String, Array[(Int, Int)]] = {
    val partitionEpochMap = new util.HashMap[String, Array[(Int, Int)]]()
    topicsRequestData.topics().forEach(topic => {
      val topicName = if (topic.name() == null) {
        metadataCache.getTopicName(topic.topicId()).get
      } else {
        topic.name()
      }
      metadataCache.numPartitions(topicName).foreach(partitionNum => {
        val partitionEpochArray = Array.fill[(Int, Int)](partitionNum)((0, 0))
        for (i <- 0 until partitionNum) {
          partitionEpochArray(i) = (i, metadataCache.getPartitionInfo(topicName, i).get.leaderEpoch())
        }
        partitionEpochMap.put(topicName, partitionEpochArray)
      })
    })

    partitionEpochMap
  }

  /**
   * Forward DeleteTopicsRequest to controller and destroy the logs of the deleted topics.
   *
   * @param request the DeleteTopicsRequest
   * @param handler the handler that may handle the DeleteTopicsRequest
   */
  private def maybeForwardTopicDeletionToController(
    request: RequestChannel.Request,
    handler: RequestChannel.Request => Unit
  ): Unit = {
    // get the map here since metadataCache has been updated before the callback.
    val topicNameToPartitionEpochsMap = getPartitionEpochSnapshotMap(request.body[DeleteTopicsRequest].data())

    def responseCallback(responseOpt: Option[AbstractResponse]): Unit = {
      responseOpt match {
        case Some(response) =>
          requestHelper.sendForwardedResponse(request, response)
          response.asInstanceOf[DeleteTopicsResponse].data().responses().forEach(result => {
            if (result.errorCode() == Errors.NONE.code()) {
              if (!metadataCache.autoMQVersion().isTopicCleanupByControllerSupported) {
                deleteTopicHandleExecutor.submit(new Runnable {
                  override def run(): Unit = {
                    topicNameToPartitionEpochsMap.get(result.name()).foreach(partitionEpochs => {
                      ElasticLogManager.destroyLog(new TopicPartition(result.name(), partitionEpochs._1), result.topicId(), partitionEpochs._2)
                    })
                  }
                })
              }
            }
          })
        case None => handleInvalidVersionsDuringForwarding(request)
      }
    }

    metadataSupport.maybeForward(request, handler, responseCallback)
  }

  private def handleExtensionRequest(request: RequestChannel.Request, requestLocal: RequestLocal): Unit = {
    def handleError(e: Throwable): Unit = {
      error(s"Unexpected error handling request ${request.requestDesc(true)} " +
        s"with context ${request.context}", e)
      requestHelper.handleError(request, e)
    }

    try {
      trace(s"Handling request:${request.requestDesc(true)} from connection ${request.context.connectionId};" +
        s"securityProtocol:${request.context.securityProtocol},principal:${request.context.principal}")

      if (!apiVersionManager.isApiEnabled(request.header.apiKey, request.header.apiVersion)) {
        // The socket server will reject APIs which are not exposed in this scope and close the connection
        // before handing them to the request handler, so this path should not be exercised in practice
        throw new IllegalStateException(s"API ${request.header.apiKey} with version ${request.header.apiVersion} is not enabled")
      }

      request.header.apiKey match {
        case ApiKeys.AUTOMQ_ZONE_ROUTER => handleZoneRouterRequest(request, requestLocal)
        case ApiKeys.AUTOMQ_GET_PARTITION_SNAPSHOT => handleGetPartitionSnapshotRequest(request, requestLocal)
        case ApiKeys.DELETE_TOPICS => maybeForwardTopicDeletionToController(request, handleDeleteTopicsRequest)
        case ApiKeys.GET_NEXT_NODE_ID => forwardToControllerOrFail(request)
        case ApiKeys.AUTOMQ_UPDATE_GROUP => handleUpdateGroupRequest(request, requestLocal)

        case _ =>
          throw new IllegalStateException("Message conversion info is recorded only for Produce/Fetch requests")
      }
    } catch {
      case e: FatalExitError => throw e
      case e: Throwable => handleError(e)
    } finally {
      // try to complete delayed action. In order to avoid conflicting locking, the actions to complete delayed requests
      // are kept in a queue. We add the logic to check the ReplicaManager queue at the end of KafkaApis.handle() and the
      // expiration thread for certain delayed operations (e.g. DelayedJoin)
      // Delayed fetches are also completed by ReplicaFetcherThread.
      replicaManager.tryCompleteActions()
      // The local completion time may be set while processing the request. Only record it if it's unset.
      if (request.apiLocalCompleteTimeNanos < 0)
        request.apiLocalCompleteTimeNanos = time.nanoseconds
    }
  }

  /**
   * Top-level method that handles all requests and multiplexes to the right api
   */
  override def handle(request: RequestChannel.Request,
    requestLocal: RequestLocal): Unit = {
    request.header.apiKey match {
      case ApiKeys.DELETE_TOPICS
           | ApiKeys.GET_NEXT_NODE_ID
           | ApiKeys.AUTOMQ_ZONE_ROUTER
           | ApiKeys.AUTOMQ_UPDATE_GROUP
           | ApiKeys.AUTOMQ_GET_PARTITION_SNAPSHOT => handleExtensionRequest(request, requestLocal)
      case _ => super.handle(request, requestLocal)
    }
  }

  protected def getCurrentLeaderForProduce(tp: TopicPartition, clientIdMetadata: ClientIdMetadata, ln: ListenerName): LeaderNode = {
    val partitionInfoOrError = replicaManager.getPartitionOrError(tp)
    val (leaderId, leaderEpoch) = partitionInfoOrError match {
      case Right(x) =>
        (x.leaderReplicaIdOpt.getOrElse(-1), x.getLeaderEpoch)
      case Left(x) =>
        debug(s"Unable to retrieve local leaderId and Epoch with error $x, falling back to metadata cache")
        metadataCache.getPartitionInfo(tp.topic, tp.partition) match {
          case Some(pinfo) => (pinfo.leader(), pinfo.leaderEpoch())
          case None => (-1, -1)
        }
    }
    LeaderNode(leaderId, leaderEpoch, OptionConverters.toScala(produceRouter.getLeaderNode(leaderId, clientIdMetadata, ln.value())))
  }

  /**
   * Handle a produce request
   */
  override def handleProduceRequest(request: RequestChannel.Request,
    requestLocal: RequestLocal): Unit = {
    val timerTotal: TimerUtil = new TimerUtil()

    val produceRequest = request.body[ProduceRequest]

    if (RequestUtils.hasTransactionalRecords(produceRequest)) {
      val isAuthorizedTransactional = produceRequest.transactionalId != null &&
        authHelper.authorize(request.context, WRITE, TRANSACTIONAL_ID, produceRequest.transactionalId)
      if (!isAuthorizedTransactional) {
        requestHelper.sendErrorResponseMaybeThrottle(request, Errors.TRANSACTIONAL_ID_AUTHORIZATION_FAILED.exception)
        return
      }
    }

    val unauthorizedTopicResponses = mutable.Map[TopicPartition, PartitionResponse]()
    val nonExistingTopicResponses = mutable.Map[TopicPartition, PartitionResponse]()
    val invalidRequestResponses = mutable.Map[TopicPartition, PartitionResponse]()
    val authorizedRequestInfo = mutable.Map[TopicPartition, MemoryRecords]()
    // cache the result to avoid redundant authorization calls
    val authorizedTopics = authHelper.filterByAuthorized(request.context, WRITE, TOPIC,
      produceRequest.data().topicData().asScala)(_.name())

    produceRequest.data.topicData.forEach(topic => topic.partitionData.forEach { partition =>
      val topicPartition = new TopicPartition(topic.name, partition.index)
      // This caller assumes the type is MemoryRecords and that is true on current serialization
      // We cast the type to avoid causing big change to code base.
      // https://issues.apache.org/jira/browse/KAFKA-10698
      val memoryRecords = partition.records.asInstanceOf[MemoryRecords]
      if (!authorizedTopics.contains(topicPartition.topic))
        unauthorizedTopicResponses += topicPartition -> new PartitionResponse(Errors.TOPIC_AUTHORIZATION_FAILED)
      else if (!metadataCache.contains(topicPartition))
        nonExistingTopicResponses += topicPartition -> new PartitionResponse(Errors.UNKNOWN_TOPIC_OR_PARTITION)
      else
        try {
          ProduceRequest.validateRecords(request.header.apiVersion, memoryRecords)
          authorizedRequestInfo += (topicPartition -> memoryRecords)
        } catch {
          case e: ApiException =>
            invalidRequestResponses += topicPartition -> new PartitionResponse(Errors.forException(e))
        }
    })

    val clientIdMetadata = ClientIdMetadata.of(request.header.clientId(), request.context.clientAddress, request.context.connectionId)

    // the callback for sending a produce response
    // The construction of ProduceResponse is able to accept auto-generated protocol data so
    // KafkaApis#handleProduceRequest should apply auto-generated protocol to avoid extra conversion.
    // https://issues.apache.org/jira/browse/KAFKA-10730
    @nowarn("cat=deprecation")
    def sendResponseCallback(responseStatus: Map[TopicPartition, PartitionResponse]): Unit = {
      val timerCallback: TimerUtil = new TimerUtil()
      PRODUCE_CALLBACK_TIME_HIST.update(timerTotal.elapsedAs(TimeUnit.MICROSECONDS))

      val mergedResponseStatus = responseStatus ++ unauthorizedTopicResponses ++ nonExistingTopicResponses ++ invalidRequestResponses
      var errorInResponse = false

      val nodeEndpoints = new mutable.HashMap[Int, Node]
      mergedResponseStatus.forKeyValue { (topicPartition, status) =>
        if (status.error != Errors.NONE) {
          errorInResponse = true
          debug("Produce request with correlation id %d from client %s on partition %s failed due to %s".format(
            request.header.correlationId,
            request.header.clientId,
            topicPartition,
            status.error.exceptionName))

          if (request.header.apiVersion >= 10) {
            status.error match {
              case Errors.NOT_LEADER_OR_FOLLOWER =>
                val leaderNode = getCurrentLeaderForProduce(topicPartition, clientIdMetadata, request.context.listenerName)
                leaderNode.node.foreach { node =>
                  nodeEndpoints.put(node.id(), node)
                }
                status.currentLeader
                  .setLeaderId(leaderNode.leaderId)
                  .setLeaderEpoch(leaderNode.leaderEpoch)
                case _ =>
            }
          }
        }
      }

      // Record both bandwidth and request quota-specific values and throttle by muting the channel if any of the quotas
      // have been violated. If both quotas have been violated, use the max throttle time between the two quotas. Note
      // that the request quota is not enforced if acks == 0.
      val timeMs = time.milliseconds()
      val requestSize = request.sizeInBytes

      // AutoMQ for Kafka inject start
      val bandwidthThrottleTimeMs = quotas.produce.maybeRecordAndGetThrottleTimeMs(request, requestSize, timeMs)
      val brokerBandwidthThrottleTimeMs = quotas.broker.maybeRecordAndGetThrottleTimeMs(QuotaType.Produce, request, requestSize, timeMs)
      val requestThrottleTimeMs =
        if (produceRequest.acks == 0) 0
        else quotas.request.maybeRecordAndGetThrottleTimeMs(request, timeMs)
      val brokerRequestThrottleTimeMs = quotas.broker.maybeRecordAndGetThrottleTimeMs(QuotaType.RequestRate, request, 1, timeMs)
      val maxThrottleTimeMs = IntStream.of(bandwidthThrottleTimeMs, requestThrottleTimeMs, brokerBandwidthThrottleTimeMs, brokerRequestThrottleTimeMs).max().orElse(0)
      if (maxThrottleTimeMs > 0) {
        request.apiThrottleTimeMs = maxThrottleTimeMs
        if (bandwidthThrottleTimeMs == maxThrottleTimeMs) {
          requestHelper.throttle(quotas.produce, request, bandwidthThrottleTimeMs)
        } else if (requestThrottleTimeMs == maxThrottleTimeMs) {
          requestHelper.throttle(quotas.request, request, requestThrottleTimeMs)
        } else if (brokerBandwidthThrottleTimeMs == maxThrottleTimeMs) {
          requestHelper.throttle(QuotaType.Produce, quotas.broker, request, brokerBandwidthThrottleTimeMs)
        } else if (brokerRequestThrottleTimeMs == maxThrottleTimeMs) {
          requestHelper.throttle(QuotaType.RequestRate, quotas.broker, request, brokerRequestThrottleTimeMs)
        }
      }
      // AutoMQ for Kafka inject end

      // Send the response immediately. In case of throttling, the channel has already been muted.
      if (produceRequest.acks == 0) {
        // no operation needed if producer request.required.acks = 0; however, if there is any error in handling
        // the request, since no response is expected by the producer, the server will close socket server so that
        // the producer client will know that some error has happened and will refresh its metadata
        if (errorInResponse) {
          val exceptionsSummary = mergedResponseStatus.map { case (topicPartition, status) =>
            topicPartition -> status.error.exceptionName
          }.mkString(", ")
          info(
            s"Closing connection due to error during produce request with correlation id ${request.header.correlationId} " +
              s"from client id ${request.header.clientId} with ack=0\n" +
              s"Topic and partition to exceptions: $exceptionsSummary"
          )
          requestChannel.closeConnection(request, new ProduceResponse(mergedResponseStatus.asJava).errorCounts)
        } else {
          // Note that although request throttling is exempt for acks == 0, the channel may be throttled due to
          // bandwidth quota violation.
          requestHelper.sendNoOpResponseExemptThrottle(request)
        }
      } else {
        requestChannel.sendResponse(request, new ProduceResponse(mergedResponseStatus.asJava, maxThrottleTimeMs, nodeEndpoints.values.toList.asJava), None)
      }

      PRODUCE_ACK_TIME_HIST.update(timerCallback.elapsedAs(TimeUnit.MICROSECONDS))
      val now = System.currentTimeMillis()
      val lastRecordTimestamp = LAST_RECORD_TIMESTAMP.get();
      if (now - lastRecordTimestamp > 60000 && LAST_RECORD_TIMESTAMP.compareAndSet(lastRecordTimestamp, now)) {
        info(s"produce cost (in microseconds), produce=${KafkaMetricsUtil.histToString(PRODUCE_TIME_HIST)} " +
          s"callback=${KafkaMetricsUtil.histToString(PRODUCE_CALLBACK_TIME_HIST)} " +
          s"ack=${KafkaMetricsUtil.histToString(PRODUCE_ACK_TIME_HIST)}")
        PRODUCE_TIME_HIST.clear()
        PRODUCE_CALLBACK_TIME_HIST.clear()
        PRODUCE_ACK_TIME_HIST.clear()
      }
    }

    def processingStatsCallback(processingStats: FetchResponseStats): Unit = {
      processingStats.forKeyValue { (tp, info) =>
        updateRecordConversionStats(request, tp, info)
      }
    }

    if (authorizedRequestInfo.isEmpty)
      sendResponseCallback(Map.empty)
    else {
      val internalTopicsAllowed = request.header.clientId == AdminUtils.ADMIN_CLIENT_ID

      def sendResponseCallbackJava(rst: util.Map[TopicPartition, PartitionResponse]): Unit = {
        sendResponseCallback(rst.asScala)
      }

      def processingStatsCallbackJava(rst: util.Map[TopicPartition, RecordValidationStats]): Unit = {
        processingStatsCallback(rst.asScala)
      }

      def doAppendRecords(): Unit = {
        produceRouter.handleProduceRequest(
          request.header.apiVersion,
          clientIdMetadata,
          produceRequest.timeout,
          produceRequest.acks,
          internalTopicsAllowed,
          produceRequest.transactionalId,
          authorizedRequestInfo.asJava,
          sendResponseCallbackJava,
          processingStatsCallbackJava,
          requestLocal,
        )

        // if the request is put into the purgatory, it will have a held reference and hence cannot be garbage collected;
        // hence we clear its data here in order to let GC reclaim its memory since it is already appended to log
        produceRequest.clearPartitionRecords()
        PRODUCE_TIME_HIST.update(timerTotal.elapsedAs(TimeUnit.MICROSECONDS))
      }
      // TODO: quick throttle when underline permit is not enough
      // TODO: isolate to a separate thread pool to avoid blocking io thread. Connection should be bind to certain async thread to keep the order
      doAppendRecords()
    }
  }

  def handleUpdateGroupRequest(request: RequestChannel.Request, requestLocal: RequestLocal): Unit = {
    val updateGroupsRequest = request.body[AutomqUpdateGroupRequest]
    groupCoordinator.updateGroup(request.context, updateGroupsRequest.data(), requestLocal.bufferSupplier)
        .whenComplete((response, ex) => {
          if (ex != null) {
            requestHelper.sendMaybeThrottle(request, updateGroupsRequest.getErrorResponse(ex))
          } else {
            requestHelper.sendMaybeThrottle(request, new AutomqUpdateGroupResponse(response))
          }
        })
  }

  def handleZoneRouterRequest(request: RequestChannel.Request, requestLocal: RequestLocal): Unit = {
    val zoneRouterRequest = request.body[AutomqZoneRouterRequest]
    produceRouter.handleZoneRouterRequest(zoneRouterRequest.data().metadata()).thenAccept(response => {
      requestChannel.sendResponse(request, response, None)
    }).exceptionally(ex => {
      handleError(request, ex)
      null
    })
  }

  def handleProduceAppendJavaCompatible(timeout: Long,
    requiredAcks: Short,
    internalTopicsAllowed: Boolean,
    transactionalId: String,
    entriesPerPartition: util.Map[TopicPartition, MemoryRecords],
    responseCallback: util.Map[TopicPartition, PartitionResponse] => Unit,
    recordValidationStatsCallback: util.Map[TopicPartition, RecordValidationStats] => Unit = _ => (),
    apiVersion: Short,
    requestLocal: RequestLocal
  ): Unit = {
    val transactionSupportedOperation = if (apiVersion > 10) genericError else defaultError
    replicaManager.handleProduceAppend(
      timeout = timeout,
      requiredAcks = requiredAcks,
      internalTopicsAllowed = internalTopicsAllowed,
      transactionalId = transactionalId,
      entriesPerPartition = entriesPerPartition.asScala,
      responseCallback = rst => responseCallback.apply(rst.asJava),
      recordValidationStatsCallback = rst => recordValidationStatsCallback.apply(rst.asJava),
      transactionSupportedOperation = transactionSupportedOperation,
      requestLocal = requestLocal,
    )
  }

  override def handleFetchRequest(request: RequestChannel.Request): Unit = {
    val versionId = request.header.apiVersion
    val clientId = request.header.clientId
    val fetchRequest = request.body[FetchRequest]
    val topicNames =
      if (fetchRequest.version() >= 13)
        metadataCache.topicIdsToNames()
      else
        Collections.emptyMap[Uuid, String]()

    val fetchData = fetchRequest.fetchData(topicNames)
    val forgottenTopics = fetchRequest.forgottenTopics(topicNames)

    val fetchContext = fetchManager.newContext(
      fetchRequest.version,
      fetchRequest.metadata,
      fetchRequest.isFromFollower,
      fetchData,
      forgottenTopics,
      topicNames)

    val erroneous = mutable.ArrayBuffer[(TopicIdPartition, FetchResponseData.PartitionData)]()
    val interesting = mutable.ArrayBuffer[(TopicIdPartition, FetchRequest.PartitionData)]()
    if (fetchRequest.isFromFollower) {
      // The follower must have ClusterAction on ClusterResource in order to fetch partition data.
      if (authHelper.authorize(request.context, CLUSTER_ACTION, CLUSTER, CLUSTER_NAME)) {
        fetchContext.foreachPartition { (topicIdPartition, data) =>
          if (topicIdPartition.topic == null)
            erroneous += topicIdPartition -> FetchResponse.partitionResponse(topicIdPartition, Errors.UNKNOWN_TOPIC_ID)
          else if (!metadataCache.contains(topicIdPartition.topicPartition))
            erroneous += topicIdPartition -> FetchResponse.partitionResponse(topicIdPartition, Errors.UNKNOWN_TOPIC_OR_PARTITION)
          else
            interesting += topicIdPartition -> data
        }
      } else {
        fetchContext.foreachPartition { (topicIdPartition, _) =>
          erroneous += topicIdPartition -> FetchResponse.partitionResponse(topicIdPartition, Errors.TOPIC_AUTHORIZATION_FAILED)
        }
      }
    } else {
      // Regular Kafka consumers need READ permission on each partition they are fetching.
      val partitionDatas = new mutable.ArrayBuffer[(TopicIdPartition, FetchRequest.PartitionData)]
      fetchContext.foreachPartition { (topicIdPartition, partitionData) =>
        if (topicIdPartition.topic == null)
          erroneous += topicIdPartition -> FetchResponse.partitionResponse(topicIdPartition, Errors.UNKNOWN_TOPIC_ID)
        else
          partitionDatas += topicIdPartition -> partitionData
      }
      val authorizedTopics = authHelper.filterByAuthorized(request.context, READ, TOPIC, partitionDatas)(_._1.topicPartition.topic)
      partitionDatas.foreach { case (topicIdPartition, data) =>
        if (!authorizedTopics.contains(topicIdPartition.topic))
          erroneous += topicIdPartition -> FetchResponse.partitionResponse(topicIdPartition, Errors.TOPIC_AUTHORIZATION_FAILED)
        else if (!metadataCache.contains(topicIdPartition.topicPartition))
          erroneous += topicIdPartition -> FetchResponse.partitionResponse(topicIdPartition, Errors.UNKNOWN_TOPIC_OR_PARTITION)
        else
          interesting += topicIdPartition -> data
      }
    }

    def maybeDownConvertStorageError(error: Errors): Errors = {
      // If consumer sends FetchRequest V5 or earlier, the client library is not guaranteed to recognize the error code
      // for KafkaStorageException. In this case the client library will translate KafkaStorageException to
      // UnknownServerException which is not retriable. We can ensure that consumer will update metadata and retry
      // by converting the KafkaStorageException to NotLeaderOrFollowerException in the response if FetchRequest version <= 5
      if (error == Errors.KAFKA_STORAGE_ERROR && versionId <= 5) {
        Errors.NOT_LEADER_OR_FOLLOWER
      } else {
        error
      }
    }

    def maybeConvertFetchedData(tp: TopicIdPartition,
                                partitionData: FetchResponseData.PartitionData): FetchResponseData.PartitionData = {
      // We will never return a logConfig when the topic is unresolved and the name is null. This is ok since we won't have any records to convert.
      val logConfig = replicaManager.getLogConfig(tp.topicPartition)

      if (logConfig.exists(_.compressionType == BrokerCompressionType.ZSTD.name) && versionId < 10) {
        trace(s"Fetching messages is disabled for ZStandard compressed partition $tp. Sending unsupported version response to $clientId.")
        FetchResponse.partitionResponse(tp, Errors.UNSUPPORTED_COMPRESSION_TYPE)
      } else {
        // Down-conversion of fetched records is needed when the on-disk magic value is greater than what is
        // supported by the fetch request version.
        // If the inter-broker protocol version is `3.0` or higher, the log config message format version is
        // always `3.0` (i.e. magic value is `v2`). As a result, we always go through the down-conversion
        // path if the fetch version is 3 or lower (in rare cases the down-conversion may not be needed, but
        // it's not worth optimizing for them).
        // If the inter-broker protocol version is lower than `3.0`, we rely on the log config message format
        // version as a proxy for the on-disk magic value to maintain the long-standing behavior originally
        // introduced in Kafka 0.10.0. An important implication is that it's unsafe to downgrade the message
        // format version after a single message has been produced (the broker would return the message(s)
        // without down-conversion irrespective of the fetch version).
        val unconvertedRecords = FetchResponse.recordsOrFail(partitionData)
        val downConvertMagic =
          logConfig.map(_.recordVersion.value).flatMap { magic =>
            if (magic > RecordBatch.MAGIC_VALUE_V0 && versionId <= 1)
              Some(RecordBatch.MAGIC_VALUE_V0)
            else if (magic > RecordBatch.MAGIC_VALUE_V1 && versionId <= 3)
              Some(RecordBatch.MAGIC_VALUE_V1)
            else
              None
          }

        downConvertMagic match {
          case Some(magic) =>
            // For fetch requests from clients, check if down-conversion is disabled for the particular partition
            if (!fetchRequest.isFromFollower && !logConfig.forall(_.messageDownConversionEnable)) {
              trace(s"Conversion to message format ${downConvertMagic.get} is disabled for partition $tp. Sending unsupported version response to $clientId.")
              FetchResponse.partitionResponse(tp, Errors.UNSUPPORTED_VERSION)
            } else {
              try {
                trace(s"Down converting records from partition $tp to message format version $magic for fetch request from $clientId")
                // Because down-conversion is extremely memory intensive, we want to try and delay the down-conversion as much
                // as possible. With KIP-283, we have the ability to lazily down-convert in a chunked manner. The lazy, chunked
                // down-conversion always guarantees that at least one batch of messages is down-converted and sent out to the
                // client.
                new FetchResponseData.PartitionData()
                  .setPartitionIndex(tp.partition)
                  .setErrorCode(maybeDownConvertStorageError(Errors.forCode(partitionData.errorCode)).code)
                  .setHighWatermark(partitionData.highWatermark)
                  .setLastStableOffset(partitionData.lastStableOffset)
                  .setLogStartOffset(partitionData.logStartOffset)
                  .setAbortedTransactions(partitionData.abortedTransactions)
                  .setRecords(new LazyDownConversionRecords(tp.topicPartition, unconvertedRecords, magic, fetchContext.getFetchOffset(tp).get, time))
                  .setPreferredReadReplica(partitionData.preferredReadReplica())
              } catch {
                case e: UnsupportedCompressionTypeException =>
                  trace("Received unsupported compression type error during down-conversion", e)
                  FetchResponse.partitionResponse(tp, Errors.UNSUPPORTED_COMPRESSION_TYPE)
              }
            }
          case None =>
            new FetchResponseData.PartitionData()
              .setPartitionIndex(tp.partition)
              .setErrorCode(maybeDownConvertStorageError(Errors.forCode(partitionData.errorCode)).code)
              .setHighWatermark(partitionData.highWatermark)
              .setLastStableOffset(partitionData.lastStableOffset)
              .setLogStartOffset(partitionData.logStartOffset)
              .setAbortedTransactions(partitionData.abortedTransactions)
              .setRecords(unconvertedRecords)
              .setPreferredReadReplica(partitionData.preferredReadReplica)
              .setDivergingEpoch(partitionData.divergingEpoch)
              .setCurrentLeader(partitionData.currentLeader())
        }
      }
    }

    // the callback for process a fetch response, invoked before throttling
    def processResponseCallback(responsePartitionData: Seq[(TopicIdPartition, FetchPartitionData)]): Unit = {
      val partitions = new util.LinkedHashMap[TopicIdPartition, FetchResponseData.PartitionData]
      val reassigningPartitions = mutable.Set[TopicIdPartition]()
      val nodeEndpoints = new mutable.HashMap[Int, Node]
      responsePartitionData.foreach { case (tp, data) =>
        val abortedTransactions = data.abortedTransactions.orElse(null)
        val lastStableOffset: Long = data.lastStableOffset.orElse(FetchResponse.INVALID_LAST_STABLE_OFFSET)
        if (data.isReassignmentFetch) reassigningPartitions.add(tp)
        val partitionData = new FetchResponseData.PartitionData()
          .setPartitionIndex(tp.partition)
          .setErrorCode(maybeDownConvertStorageError(data.error).code)
          .setHighWatermark(data.highWatermark)
          .setLastStableOffset(lastStableOffset)
          .setLogStartOffset(data.logStartOffset)
          .setAbortedTransactions(abortedTransactions)
          .setRecords(data.records)
          .setPreferredReadReplica(data.preferredReadReplica.orElse(FetchResponse.INVALID_PREFERRED_REPLICA_ID))

        if (versionId >= 16) {
          data.error match {
            case Errors.NOT_LEADER_OR_FOLLOWER | Errors.FENCED_LEADER_EPOCH =>
              val leaderNode = getCurrentLeader(tp.topicPartition(), request.context.listenerName)
              leaderNode.node.foreach { node =>
                nodeEndpoints.put(node.id(), node)
              }
              partitionData.currentLeader()
                .setLeaderId(leaderNode.leaderId)
                .setLeaderEpoch(leaderNode.leaderEpoch)
            case _ =>
          }
        }

        data.divergingEpoch.ifPresent(partitionData.setDivergingEpoch(_))
        partitions.put(tp, partitionData)
      }
      erroneous.foreach { case (tp, data) => partitions.put(tp, data) }

      var unconvertedFetchResponse: FetchResponse = null

      def createResponse(throttleTimeMs: Int): FetchResponse = {
        // Down-convert messages for each partition if required
        val convertedData = new util.LinkedHashMap[TopicIdPartition, FetchResponseData.PartitionData]
        unconvertedFetchResponse.data().responses().forEach { topicResponse =>
          topicResponse.partitions().forEach { unconvertedPartitionData =>
            val tp = new TopicIdPartition(topicResponse.topicId, new TopicPartition(topicResponse.topic, unconvertedPartitionData.partitionIndex()))
            val error = Errors.forCode(unconvertedPartitionData.errorCode)
            if (error != Errors.NONE)
              debug(s"Fetch request with correlation id ${request.header.correlationId} from client $clientId " +
                s"on partition $tp failed due to ${error.exceptionName}")
            convertedData.put(tp, maybeConvertFetchedData(tp, unconvertedPartitionData))
          }
        }

        // Prepare fetch response from converted data
        val response =
          FetchResponse.of(unconvertedFetchResponse.error, throttleTimeMs, unconvertedFetchResponse.sessionId, convertedData, nodeEndpoints.values.toList.asJava)
        // record the bytes out metrics only when the response is being sent
        response.data.responses.forEach { topicResponse =>
          topicResponse.partitions.forEach { data =>
            // If the topic name was not known, we will have no bytes out.
            if (topicResponse.topic != null) {
              val tp = new TopicIdPartition(topicResponse.topicId, new TopicPartition(topicResponse.topic, data.partitionIndex))
              brokerTopicStats.updateBytesOut(tp.topicPartition(), fetchRequest.isFromFollower, reassigningPartitions.contains(tp), FetchResponse.recordsSize(data))
            }
          }
        }
        response
      }

      def updateConversionStats(send: Send): Unit = {
        send match {
          case send: MultiRecordsSend if send.recordConversionStats != null =>
            send.recordConversionStats.asScala.toMap.foreach {
              case (tp, stats) => updateRecordConversionStats(request, tp, stats)
            }
          case send: NetworkSend =>
            updateConversionStats(send.send())
          case _ =>
        }
      }

      def release(): Unit = {
        partitions.values().forEach(data => {
          if (data.records() != null && data.records().isInstanceOf[PooledResource]) {
            data.records().asInstanceOf[PooledResource].release()
          }
        })
      }

      if (fetchRequest.isFromFollower) {
        // We've already evaluated against the quota and are good to go. Just need to record it now.
        unconvertedFetchResponse = fetchContext.updateAndGenerateResponseData(partitions)
        val responseSize = KafkaApis.sizeOfThrottledPartitions(versionId, unconvertedFetchResponse, quotas.leader)
        quotas.leader.record(responseSize)
        val responsePartitionsSize = unconvertedFetchResponse.data().responses().stream().mapToInt(_.partitions().size()).sum()
        trace(s"Sending Fetch response with partitions.size=$responsePartitionsSize, " +
          s"metadata=${unconvertedFetchResponse.sessionId}")
        requestHelper.sendResponseExemptThrottle(request, createResponse(0), Some(updateConversionStats))
      } else {
        // Fetch size used to determine throttle time is calculated before any down conversions.
        // This may be slightly different from the actual response size. But since down conversions
        // result in data being loaded into memory, we should do this only when we are not going to throttle.
        //
        // Record both bandwidth and request quota-specific values and throttle by muting the channel if any of the
        // quotas have been violated. If both quotas have been violated, use the max throttle time between the two
        // quotas. When throttled, we unrecord the recorded bandwidth quota value
        val responseSize = fetchContext.getResponseSize(partitions, versionId)
        val timeMs = time.milliseconds()

        // AutoMQ for Kafka inject start
        val isSlowRead = !ReadHint.isFastRead

        val requestThrottleTimeMs = quotas.request.maybeRecordAndGetThrottleTimeMs(request, timeMs)
        val bandwidthThrottleTimeMs = quotas.fetch.maybeRecordAndGetThrottleTimeMs(request, responseSize, timeMs)
        val brokerBandwidthThrottleTimeMs = quotas.broker.maybeRecordAndGetThrottleTimeMs(QuotaType.Fetch, request, responseSize, timeMs)
        val brokerSlowFetchThrottleTimeMs = if (isSlowRead) quotas.broker.maybeRecordAndGetThrottleTimeMs(QuotaType.SlowFetch, request, responseSize, timeMs) else 0
        val brokerRequestThrottleTimeMs = quotas.broker.maybeRecordAndGetThrottleTimeMs(QuotaType.RequestRate, request, 1, timeMs)

        val maxThrottleTimeMs = IntStream.of(bandwidthThrottleTimeMs, requestThrottleTimeMs, brokerBandwidthThrottleTimeMs, brokerSlowFetchThrottleTimeMs, brokerRequestThrottleTimeMs).max().orElse(0)
        if (maxThrottleTimeMs > 0) {
          request.apiThrottleTimeMs = maxThrottleTimeMs
          // Even if we need to throttle for request quota violation, we should "unrecord" the already recorded value
          // from the fetch quota because we are going to return an empty response.
          quotas.fetch.unrecordQuotaSensor(request, responseSize, timeMs)
          quotas.broker.unrecordQuotaSensor(QuotaType.Fetch, responseSize, timeMs)
          if (isSlowRead) {
            quotas.broker.unrecordQuotaSensor(QuotaType.SlowFetch, responseSize, timeMs)
          }
          if (bandwidthThrottleTimeMs == maxThrottleTimeMs) {
            requestHelper.throttle(quotas.fetch, request, bandwidthThrottleTimeMs)
          } else if (requestThrottleTimeMs == maxThrottleTimeMs) {
            requestHelper.throttle(quotas.request, request, requestThrottleTimeMs)
          } else if (brokerBandwidthThrottleTimeMs == maxThrottleTimeMs) {
            requestHelper.throttle(QuotaType.Fetch, quotas.broker, request, brokerBandwidthThrottleTimeMs)
          } else if (brokerSlowFetchThrottleTimeMs == maxThrottleTimeMs) {
            requestHelper.throttle(QuotaType.SlowFetch, quotas.broker, request, brokerSlowFetchThrottleTimeMs)
          } else if (brokerRequestThrottleTimeMs == maxThrottleTimeMs) {
            requestHelper.throttle(QuotaType.RequestRate, quotas.broker, request, brokerRequestThrottleTimeMs)
          }
          // AutoMQ for Kafka inject end

          // If throttling is required, return an empty response.
          unconvertedFetchResponse = fetchContext.getThrottledResponse(maxThrottleTimeMs)
          release()
        } else {
          // Get the actual response. This will update the fetch context.
          unconvertedFetchResponse = fetchContext.updateAndGenerateResponseData(partitions)
          val responsePartitionsSize = unconvertedFetchResponse.data().responses().stream().mapToInt(_.partitions().size()).sum()
          trace(s"Sending Fetch response with partitions.size=$responsePartitionsSize, " +
            s"metadata=${unconvertedFetchResponse.sessionId}")
        }

        // Send the response immediately.
        requestChannel.sendResponse(request, createResponse(maxThrottleTimeMs), Some(updateConversionStats))
      }
    }

    if (interesting.isEmpty) {
      processResponseCallback(Seq.empty)
    } else {
      // for fetch from consumer, cap fetchMaxBytes to the maximum bytes that could be fetched without being throttled given
      // no bytes were recorded in the recent quota window
      // trying to fetch more bytes would result in a guaranteed throttling potentially blocking consumer progress
      val maxQuotaWindowBytes = if (fetchRequest.isFromFollower)
        Int.MaxValue
      else {
        val maxValue = quotas.fetch.getMaxValueInQuotaWindow(request.session, clientId).toInt
        val brokerMaxValue = quotas.broker.getMaxValueInQuotaWindow(QuotaType.Fetch, request).toInt
        math.min(maxValue, brokerMaxValue)
      }


      val fetchMaxBytes = Math.min(Math.min(fetchRequest.maxBytes, config.fetchMaxBytes), maxQuotaWindowBytes)
      val fetchMinBytes = Math.min(fetchRequest.minBytes, fetchMaxBytes)

      val clientMetadata: Optional[ClientMetadata] = if (versionId >= 11) {
        // Fetch API version 11 added preferred replica logic
        Optional.of(new DefaultClientMetadata(
          fetchRequest.rackId,
          clientId,
          request.context.clientAddress,
          request.context.principal,
          request.context.listenerName.value))
      } else {
        Optional.empty()
      }

      val params = new FetchParams(
        versionId,
        fetchRequest.replicaId,
        fetchRequest.replicaEpoch,
        fetchRequest.maxWait,
        fetchMinBytes,
        fetchMaxBytes,
        FetchIsolation.of(fetchRequest),
        clientMetadata
      )

      // call the replica manager to fetch messages from the local replica
      replicaManager.fetchMessages(
        params = params,
        fetchInfos = interesting,
        quota = replicationQuota(fetchRequest),
        responseCallback = processResponseCallback,
      )
    }
  }

  override def handleListOffsetRequest(request: RequestChannel.Request): Unit = {
    // isolate to a separate thread pool to avoid blocking io thread (PRODUCE use io thread).
    listOffsetHandleExecutor.execute(() => super.handleListOffsetRequest(request))
  }

  override protected def metadataTopicsInterceptor(clientId: String, listenerName: String, topics: util.List[MetadataResponseData.MetadataResponseTopic]): util.List[MetadataResponseData.MetadataResponseTopic] = {
    produceRouter.handleMetadataResponse(clientId, topics)
  }

  def handleGetPartitionSnapshotRequest(request: RequestChannel.Request, requestLocal: RequestLocal): Unit = {
    val req = request.body[AutomqGetPartitionSnapshotRequest]
    val resp = replicaManager.asInstanceOf[ElasticReplicaManager].handleGetPartitionSnapshotRequest(req)
    requestHelper.sendMaybeThrottle(request, resp)
  }

  def setProduceRouter(produceRouter: ProduceRouter): Unit = {
    this.produceRouter = produceRouter
  }

}
