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

package kafka.server.streamaspect.extension

import kafka.network.RequestChannel
import kafka.server.{HostedPartition, RequestLocal}

import org.apache.kafka.common.{TopicPartition, Uuid}
import org.apache.kafka.common.requests.AbstractResponse

import java.util.Optional
import java.util.ServiceLoader

import scala.jdk.CollectionConverters.IteratorHasAsScala

/**
 * Broker-side extension SPI.
 *
 * Implementations are created by [[BrokerExtensionHandleProvider]], initialized with
 * [[BrokerExtensionContext]], and invoked by [[BrokerExtensionHandleDispatcher]].
 */
trait BrokerExtensionHandle {
  def init(ops: BrokerExtensionContext): Unit = {
  }

  def handle(request: RequestChannel.Request, requestLocal: RequestLocal): Unit
}

trait BrokerExtensionHandleProvider {
  def create(): BrokerExtensionHandle
}

trait BrokerExtensionContext {
  def forwardToControllerOrFail(request: RequestChannel.Request): Unit

  def maybeForward(request: RequestChannel.Request,
    handler: RequestChannel.Request => Unit,
    responseCallback: Option[AbstractResponse] => Unit): Unit

  def sendForwardedResponse(request: RequestChannel.Request, response: AbstractResponse): Unit

  def sendResponseMaybeThrottle(request: RequestChannel.Request,
    responseBuilder: Int => AbstractResponse): Unit

  def handleError(request: RequestChannel.Request, t: Throwable): Unit

  def handleInvalidVersionsDuringForwarding(request: RequestChannel.Request): Unit

  def getTopicName(topicId: Uuid): Optional[String]

  def getPartition(topicPartition: TopicPartition): HostedPartition
}

trait BrokerExtensionHandleDispatcher {
  import BrokerExtensionHandleDispatcher.Result

  def handle(request: RequestChannel.Request, requestLocal: RequestLocal): Result
}

object BrokerExtensionHandleDispatcher {
  sealed trait Result
  case object Handled extends Result
  case object NotHandled extends Result

  def load(
    context: BrokerExtensionContext,
    loader: ServiceLoader[BrokerExtensionHandleProvider] = ServiceLoader.load(classOf[BrokerExtensionHandleProvider])
  ): BrokerExtensionHandleDispatcher = {
    loadFromProviders(context, loader.iterator().asScala.toSeq)
  }

  def loadFromProviders(
    context: BrokerExtensionContext,
    providers: Iterable[BrokerExtensionHandleProvider]
  ): BrokerExtensionHandleDispatcher = {
    val loadedProviders = providers.toSeq
    if (loadedProviders.size > 1) {
      val providerNames = loadedProviders.map(_.getClass.getName).mkString(", ")
      throw new IllegalStateException(
        s"Only one BrokerExtensionHandleProvider is supported, found ${loadedProviders.size}: ${providerNames}")
    }

    loadedProviders.headOption match {
      case Some(provider) =>
        val handle = provider.create()
        handle.init(context)
        (request: RequestChannel.Request, requestLocal: RequestLocal) => {
          handle.handle(request, requestLocal)
          Handled
        }
      case None =>
        (_: RequestChannel.Request, _: RequestLocal) => NotHandled
    }
  }
}
