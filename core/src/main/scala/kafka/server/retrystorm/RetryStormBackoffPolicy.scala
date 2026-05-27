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

package kafka.server.retrystorm

import kafka.automq.retrystorm.{RetryStormBackoffConfig, RetryStormBackoffStateStore}
import org.apache.kafka.common.protocol.ApiKeys

sealed trait BackoffAction

object BackoffAction {
  case object Immediate extends BackoffAction
  case object Delayed extends BackoffAction
}

case class RequestSummary()

case class BackoffContext(clientScope: String, nowMs: Long)

case class ResourceResult(resourceKey: String,
                          valid: Boolean,
                          delayableTransient: Boolean,
                          protective: Boolean)

case class ResponseSummary(resources: Seq[ResourceResult])

case class BackoffDecision(action: BackoffAction, delayMs: Long = 0L, reason: String = "") {
  def delayed: Boolean = action == BackoffAction.Delayed
}

/**
 * Evaluates schema-independent retry storm summaries and updates per-resource backoff state.
 */
class RetryStormBackoffPolicy(config: RetryStormBackoffConfig, stateStore: RetryStormBackoffStateStore) {

  def evaluate(apiKey: ApiKeys,
               requestSummary: RequestSummary,
               responseSummary: ResponseSummary,
               context: BackoffContext): BackoffDecision = {
    val _ = requestSummary
    if (!config.enabled()) {
      return BackoffDecision(BackoffAction.Immediate)
    }

    val validResources = responseSummary.resources.filter(_.valid)
    if (validResources.nonEmpty) {
      validResources.foreach(resource => stateStore.clear(backoffKey(apiKey, resource, context)))
      return BackoffDecision(BackoffAction.Immediate)
    }

    val decisions = responseSummary.resources
      .filter(resource => resource.delayableTransient || resource.protective)
      .map { resource =>
        stateStore.recordAndDecide(
          backoffKey(apiKey, resource, context),
          new RetryStormBackoffStateStore.ErrorClassSet(resource.delayableTransient, resource.protective),
          context.nowMs,
          config.maxDelayMs()
        )
      }

    val delayed = decisions.filter(_.delayed())
    if (delayed.isEmpty) {
      BackoffDecision(BackoffAction.Immediate)
    } else {
      val maxDelayMs = delayed.map(_.delayMs()).max
      val reason = delayed.flatMap(_.reason().split(",")).filter(_.nonEmpty).toSet.toSeq.sorted.mkString(",")
      BackoffDecision(BackoffAction.Delayed, maxDelayMs, reason)
    }
  }

  private def backoffKey(apiKey: ApiKeys,
                         resource: ResourceResult,
                         context: BackoffContext): RetryStormBackoffStateStore.BackoffKey = {
    new RetryStormBackoffStateStore.BackoffKey(apiKey.id, resource.resourceKey, context.clientScope)
  }
}
