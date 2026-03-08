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

package kafka.server.streamaspect.extension

import kafka.network.RequestChannel
import kafka.server.RequestLocal

import java.util.ServiceLoader

import scala.jdk.CollectionConverters.IteratorHasAsScala

trait BrokerExtensionHandleDispatcher {
  import BrokerExtensionHandleDispatcher.Result

  def handle(request: RequestChannel.Request, requestLocal: RequestLocal): Result
}

object BrokerExtensionHandleDispatcher {
  sealed trait Result
  case object Handled extends Result
  case object NotHandled extends Result

  def load(
    ops: BrokerHandleOps,
    loader: ServiceLoader[BrokerExtensionHandleProvider] = ServiceLoader.load(classOf[BrokerExtensionHandleProvider])
  ): BrokerExtensionHandleDispatcher = {
    loadFromProviders(ops, loader.iterator().asScala.toSeq)
  }

  def loadFromProviders(
    ops: BrokerHandleOps,
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
        handle.init(ops)
        (request: RequestChannel.Request, requestLocal: RequestLocal) => {
          handle.handle(request, requestLocal)
          Handled
        }
      case None =>
        (_: RequestChannel.Request, _: RequestLocal) => NotHandled
    }
  }
}
