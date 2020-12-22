/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package kafka.tools

import kafka.network.RequestChannel
import kafka.network.RequestConvertToJson
import kafka.server.ApiRequestHandler
import kafka.utils.Logging
import org.apache.kafka.common.internals.FatalExitError
import org.apache.kafka.common.message.{BeginQuorumEpochResponseData, EndQuorumEpochResponseData, FetchResponseData, VoteResponseData}
import org.apache.kafka.common.protocol.{ApiKeys, ApiMessage, Errors}
import org.apache.kafka.common.record.BaseRecords
import org.apache.kafka.common.requests.{AbstractRequest, AbstractResponse, BeginQuorumEpochResponse, EndQuorumEpochResponse, FetchResponse, VoteResponse}
import org.apache.kafka.common.utils.Time
import org.apache.kafka.raft.{KafkaRaftClient, RaftRequest}

import scala.jdk.CollectionConverters._

/**
 * Simple request handler implementation for use by [[TestRaftServer]].
 */
class TestRaftRequestHandler(
  raftClient: KafkaRaftClient[_],
  requestChannel: RequestChannel,
  time: Time,
) extends ApiRequestHandler with Logging {

  override def handle(request: RequestChannel.Request): Unit = {
    try {
      trace(s"Handling request:${request.requestDesc(true)} from connection ${request.context.connectionId};" +
        s"securityProtocol:${request.context.securityProtocol},principal:${request.context.principal}")
      request.header.apiKey match {
        case ApiKeys.VOTE => handleVote(request)
        case ApiKeys.BEGIN_QUORUM_EPOCH => handleBeginQuorumEpoch(request)
        case ApiKeys.END_QUORUM_EPOCH => handleEndQuorumEpoch(request)
        case ApiKeys.FETCH => handleFetch(request)

        case _ => throw new IllegalArgumentException(s"Unsupported api key: ${request.header.apiKey}")
      }
    } catch {
      case e: FatalExitError => throw e
      case e: Throwable => handleError(request, e)
    } finally {
      // The local completion time may be set while processing the request. Only record it if it's unset.
      if (request.apiLocalCompleteTimeNanos < 0)
        request.apiLocalCompleteTimeNanos = time.nanoseconds
    }
  }

  private def handleVote(request: RequestChannel.Request): Unit = {
    handle(request, response => new VoteResponse(response.asInstanceOf[VoteResponseData]))
  }

  private def handleBeginQuorumEpoch(request: RequestChannel.Request): Unit = {
    handle(request, response => new BeginQuorumEpochResponse(response.asInstanceOf[BeginQuorumEpochResponseData]))
  }

  private def handleEndQuorumEpoch(request: RequestChannel.Request): Unit = {
    handle(request, response => new EndQuorumEpochResponse(response.asInstanceOf[EndQuorumEpochResponseData]))
  }

  private def handleFetch(request: RequestChannel.Request): Unit = {
    handle(request, response => new FetchResponse[BaseRecords](response.asInstanceOf[FetchResponseData]))
  }

  private def handle(
    request: RequestChannel.Request,
    buildResponse: ApiMessage => AbstractResponse
  ): Unit = {
    val requestBody = request.body[AbstractRequest]
    val inboundRequest = new RaftRequest.Inbound(
      request.header.correlationId,
      requestBody.data,
      time.milliseconds()
    )

    inboundRequest.completion.whenComplete((response, exception) => {
      val res = if (exception != null) {
        requestBody.getErrorResponse(exception)
      } else {
        buildResponse(response.data)
      }
      sendResponse(request, Some(res))
    })

    raftClient.handle(inboundRequest)
  }

  private def handleError(request: RequestChannel.Request, err: Throwable): Unit = {
    error("Error when handling request: " +
      s"clientId=${request.header.clientId}, " +
      s"correlationId=${request.header.correlationId}, " +
      s"api=${request.header.apiKey}, " +
      s"version=${request.header.apiVersion}, " +
      s"body=${request.body[AbstractRequest]}", err)

    val requestBody = request.body[AbstractRequest]
    val response = requestBody.getErrorResponse(0, err)
    if (response == null)
      closeConnection(request, requestBody.errorCounts(err))
    else
      sendResponse(request, Some(response))
  }

  private def closeConnection(request: RequestChannel.Request, errorCounts: java.util.Map[Errors, Integer]): Unit = {
    // This case is used when the request handler has encountered an error, but the client
    // does not expect a response (e.g. when produce request has acks set to 0)
    requestChannel.updateErrorMetrics(request.header.apiKey, errorCounts.asScala)
    requestChannel.sendResponse(new RequestChannel.CloseConnectionResponse(request))
  }

  private def sendResponse(request: RequestChannel.Request,
                           responseOpt: Option[AbstractResponse]): Unit = {
    // Update error metrics for each error code in the response including Errors.NONE
    responseOpt.foreach(response => requestChannel.updateErrorMetrics(request.header.apiKey, response.errorCounts.asScala))

    val response = responseOpt match {
      case Some(response) =>
        val responseSend = request.context.buildResponseSend(response)
        val responseString =
          if (RequestChannel.isRequestLoggingEnabled) Some(RequestConvertToJson.response(response, request.context.apiVersion))
          else None
        new RequestChannel.SendResponse(request, responseSend, responseString, None)
      case None =>
        new RequestChannel.NoOpResponse(request)
    }
    sendResponse(response)
  }

  private def sendResponse(response: RequestChannel.Response): Unit = {
    requestChannel.sendResponse(response)
  }

}
