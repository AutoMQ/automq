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

import kafka.utils.MockTime
import org.apache.kafka.clients.{ClientResponse, MockClient, NodeApiVersions}
import org.apache.kafka.common.protocol.Errors
import org.apache.kafka.common.requests.AbstractRequest

class MockBrokerToControllerChannelManager(
  val client: MockClient,
  time: MockTime,
  controllerNodeProvider: ControllerNodeProvider,
  controllerApiVersions: NodeApiVersions = NodeApiVersions.create(),
  val retryTimeoutMs: Int = 60000,
  val requestTimeoutMs: Int = 30000
) extends BrokerToControllerChannelManager {
  private val unsentQueue = new java.util.ArrayDeque[BrokerToControllerQueueItem]()

  client.setNodeApiVersions(controllerApiVersions)

  override def start(): Unit = {}

  override def shutdown(): Unit = {}

  override def sendRequest(
    request: AbstractRequest.Builder[_ <: AbstractRequest],
    callback: ControllerRequestCompletionHandler
  ): Unit = {
    unsentQueue.add(BrokerToControllerQueueItem(
      createdTimeMs = time.milliseconds(),
      request = request,
      callback = callback
    ))
  }

  override def controllerApiVersions(): Option[NodeApiVersions] = {
    Some(controllerApiVersions)
  }

  private[server] def handleResponse(request: BrokerToControllerQueueItem)(response: ClientResponse): Unit = {
    if (response.authenticationException != null || response.versionMismatch != null) {
      request.callback.onComplete(response)
    } else if (response.wasDisconnected() || response.responseBody.errorCounts.containsKey(Errors.NOT_CONTROLLER)) {
      unsentQueue.addFirst(request)
    } else {
      request.callback.onComplete(response)
    }
  }

  def poll(): Unit = {
    val unsentIterator = unsentQueue.iterator()
    var canSend = true

    while (canSend && unsentIterator.hasNext) {
      val queueItem = unsentIterator.next()
      val elapsedTimeMs = time.milliseconds() - queueItem.createdTimeMs
      if (elapsedTimeMs >= retryTimeoutMs) {
        queueItem.callback.onTimeout()
        unsentIterator.remove()
      } else {
        controllerNodeProvider.get() match {
          case Some(controller) if client.ready(controller, time.milliseconds()) =>
            val clientRequest = client.newClientRequest(
              controller.idString,
              queueItem.request,
              queueItem.createdTimeMs,
              true, // we expect response,
              requestTimeoutMs,
              handleResponse(queueItem)
            )
            client.send(clientRequest, time.milliseconds())
            unsentIterator.remove()

          case _ => canSend = false
        }
      }
    }

    client.poll(0L, time.milliseconds())
  }

}
