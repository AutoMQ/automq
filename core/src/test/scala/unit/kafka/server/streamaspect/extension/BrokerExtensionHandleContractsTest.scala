/*
 * Copyright 2026, AutoMQ HK Limited.
 *
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

package unit.kafka.server.streamaspect.extension

import kafka.network.RequestChannel
import kafka.server.{HostedPartition, RequestLocal}
import kafka.server.streamaspect.extension.{BrokerExtensionHandle, BrokerExtensionHandleProvider, BrokerExtensionContext}
import org.apache.kafka.common.{TopicPartition, Uuid}
import org.junit.jupiter.api.Assertions.assertNotNull
import org.junit.jupiter.api.Test

import java.util.Optional

class BrokerExtensionHandleContractsTest {

  @Test
  def shouldSupportDefaultInitHook(): Unit = {
    var initialized = false
    val handle = new BrokerExtensionHandle {
      override def init(ops: BrokerExtensionContext): Unit = {
        initialized = true
      }

      override def handle(request: RequestChannel.Request, requestLocal: RequestLocal): Unit = {
      }
    }

    handle.init(new BrokerExtensionContext {
      override def forwardToControllerOrFail(request: RequestChannel.Request): Unit = ()
      override def maybeForward(request: RequestChannel.Request, handler: RequestChannel.Request => Unit,
        cb: Option[org.apache.kafka.common.requests.AbstractResponse] => Unit): Unit = ()
      override def sendForwardedResponse(request: RequestChannel.Request,
        response: org.apache.kafka.common.requests.AbstractResponse): Unit = ()
      override def sendResponseMaybeThrottle(request: RequestChannel.Request,
        responseBuilder: Int => org.apache.kafka.common.requests.AbstractResponse): Unit = ()
      override def handleError(request: RequestChannel.Request, t: Throwable): Unit = ()
      override def handleInvalidVersionsDuringForwarding(request: RequestChannel.Request): Unit = ()
      override def getTopicName(topicId: Uuid): Optional[String] = Optional.empty()
      override def getPartition(topicPartition: TopicPartition): HostedPartition = HostedPartition.None
    })
    assert(initialized)
  }

  @Test
  def shouldAllowProviderToCreateHandle(): Unit = {
    val provider = new BrokerExtensionHandleProvider {
      override def create(): BrokerExtensionHandle = new BrokerExtensionHandle {
        override def handle(request: RequestChannel.Request, requestLocal: RequestLocal): Unit = ()
      }
    }

    assertNotNull(provider.create())
  }
}
