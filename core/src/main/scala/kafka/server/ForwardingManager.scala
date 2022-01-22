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

package kafka.server

import java.nio.ByteBuffer

import kafka.network.RequestChannel
import kafka.utils.Logging
import org.apache.kafka.clients.{ClientResponse, NodeApiVersions}
import org.apache.kafka.common.errors.TimeoutException
import org.apache.kafka.common.protocol.Errors
import org.apache.kafka.common.requests.{AbstractRequest, AbstractResponse, EnvelopeRequest, EnvelopeResponse, RequestContext, RequestHeader}

import scala.compat.java8.OptionConverters._

trait ForwardingManager {
  def forwardRequest(
    originalRequest: RequestChannel.Request,
    responseCallback: Option[AbstractResponse] => Unit
  ): Unit = {
    val buffer = originalRequest.buffer.duplicate()
    buffer.flip()
    forwardRequest(originalRequest.context,
      buffer,
      originalRequest.body[AbstractRequest],
      () => originalRequest.toString,
      responseCallback)
  }

  def forwardRequest(
    originalRequest: RequestChannel.Request,
    newRequestBody: AbstractRequest,
    responseCallback: Option[AbstractResponse] => Unit
  ): Unit = {
    val buffer = newRequestBody.serializeWithHeader(originalRequest.header)
    forwardRequest(originalRequest.context,
      buffer,
      newRequestBody,
      () => originalRequest.toString,
      responseCallback)
  }

  /**
   * Forward given request to the active controller.
   *
   * @param requestContext      The request context of the original envelope request.
   * @param requestBufferCopy   The request buffer we want to send. This should not be the original
   *                            byte buffer from the envelope request, since we will be mutating
   *                            the position and limit fields. It should be a copy.
   * @param requestBody         The AbstractRequest we are sending.
   * @param requestToString     A callback which can be invoked to produce a human-readable decription
   *                            of the request.
   * @param responseCallback    A callback which takes in an `Option[AbstractResponse]`.
   *                            We will call this function with Some(x) after the controller responds with x.
   *                            Or, if the controller doesn't support the request version, we will complete
   *                            the callback with None.
   */
  def forwardRequest(
    requestContext: RequestContext,
    requestBufferCopy: ByteBuffer,
    requestBody: AbstractRequest,
    requestToString: () => String,
    responseCallback: Option[AbstractResponse] => Unit
  ): Unit

  def controllerApiVersions: Option[NodeApiVersions]
}

object ForwardingManager {
  def apply(
    channelManager: BrokerToControllerChannelManager
  ): ForwardingManager = {
    new ForwardingManagerImpl(channelManager)
  }

  private[server] def buildEnvelopeRequest(context: RequestContext,
                                           forwardRequestBuffer: ByteBuffer): EnvelopeRequest.Builder = {
    val principalSerde = context.principalSerde.asScala.getOrElse(
      throw new IllegalArgumentException(s"Cannot deserialize principal from request context $context " +
        "since there is no serde defined")
    )
    val serializedPrincipal = principalSerde.serialize(context.principal)
    new EnvelopeRequest.Builder(
      forwardRequestBuffer,
      serializedPrincipal,
      context.clientAddress.getAddress
    )
  }
}

class ForwardingManagerImpl(
  channelManager: BrokerToControllerChannelManager
) extends ForwardingManager with Logging {

  override def forwardRequest(
    requestContext: RequestContext,
    requestBufferCopy: ByteBuffer,
    requestBody: AbstractRequest,
    requestToString: () => String,
    responseCallback: Option[AbstractResponse] => Unit
  ): Unit = {
    val envelopeRequest = ForwardingManager.buildEnvelopeRequest(requestContext, requestBufferCopy)

    class ForwardingResponseHandler extends ControllerRequestCompletionHandler {
      override def onComplete(clientResponse: ClientResponse): Unit = {

        if (clientResponse.versionMismatch != null) {
          debug(s"Returning `UNKNOWN_SERVER_ERROR` in response to ${requestToString()} " +
            s"due to unexpected version error", clientResponse.versionMismatch)
          responseCallback(Some(requestBody.getErrorResponse(Errors.UNKNOWN_SERVER_ERROR.exception)))
        } else if (clientResponse.authenticationException != null) {
          debug(s"Returning `UNKNOWN_SERVER_ERROR` in response to ${requestToString()} " +
            s"due to authentication error", clientResponse.authenticationException)
          responseCallback(Some(requestBody.getErrorResponse(Errors.UNKNOWN_SERVER_ERROR.exception)))
        } else {
          val envelopeResponse = clientResponse.responseBody.asInstanceOf[EnvelopeResponse]
          val envelopeError = envelopeResponse.error()

          // Unsupported version indicates an incompatibility between controller and client API versions. This
          // could happen when the controller changed after the connection was established. The forwarding broker
          // should close the connection with the client and let it reinitialize the connection and refresh
          // the controller API versions.
          if (envelopeError == Errors.UNSUPPORTED_VERSION) {
            responseCallback(None)
          } else {
            val response = if (envelopeError != Errors.NONE) {
              // A general envelope error indicates broker misconfiguration (e.g. the principal serde
              // might not be defined on the receiving broker). In this case, we do not return
              // the error directly to the client since it would not be expected. Instead we
              // return `UNKNOWN_SERVER_ERROR` so that the user knows that there is a problem
              // on the broker.
              debug(s"Forwarded request ${requestToString()} failed with an error in the envelope response $envelopeError")
              requestBody.getErrorResponse(Errors.UNKNOWN_SERVER_ERROR.exception)
            } else {
              parseResponse(envelopeResponse.responseData, requestBody, requestContext.header)
            }
            responseCallback(Option(response))
          }
        }
      }

      override def onTimeout(): Unit = {
        debug(s"Forwarding of the request ${requestToString()} failed due to timeout exception")
        val response = requestBody.getErrorResponse(new TimeoutException())
        responseCallback(Option(response))
      }
    }

    channelManager.sendRequest(envelopeRequest, new ForwardingResponseHandler)
  }

  override def controllerApiVersions: Option[NodeApiVersions] =
    channelManager.controllerApiVersions()

  private def parseResponse(
    buffer: ByteBuffer,
    request: AbstractRequest,
    header: RequestHeader
  ): AbstractResponse = {
    try {
      AbstractResponse.parseResponse(buffer, header)
    } catch {
      case e: Exception =>
        error(s"Failed to parse response from envelope for request with header $header", e)
        request.getErrorResponse(Errors.UNKNOWN_SERVER_ERROR.exception)
    }
  }
}
