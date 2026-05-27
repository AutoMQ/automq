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
import org.junit.jupiter.api.Assertions._
import org.junit.jupiter.api.Test
import scala.jdk.CollectionConverters._

class RetryStormBackoffPolicyTest {

  @Test
  def testDisabledConfigReturnsImmediateWithoutStateUpdate(): Unit = {
    val config = new RetryStormBackoffConfig(false, 1000L)
    val policy = newPolicy(config)
    val context = BackoffContext("connection-1", 1000L)
    val response = ResponseSummary(Seq(ResourceResult("topic-0", valid = false, delayableTransient = true, protective = true)))

    assertEquals(BackoffAction.Immediate, policy.evaluate(ApiKeys.PRODUCE, RequestSummary(), response, context).action)

    config.update(Map(kafka.automq.AutoMQConfig.RETRY_STORM_BACKOFF_ENABLED_CONFIG -> true).asJava)
    assertEquals(BackoffAction.Immediate, policy.evaluate(ApiKeys.PRODUCE, RequestSummary(), response, context.copy(nowMs = 1001L)).action)
  }

  @Test
  def testValidResultClearsState(): Unit = {
    val policy = newPolicy()
    val error = ResponseSummary(Seq(ResourceResult("topic-0", valid = false, delayableTransient = true, protective = true)))
    val valid = ResponseSummary(Seq(ResourceResult("topic-0", valid = true, delayableTransient = false, protective = false)))

    assertEquals(BackoffAction.Immediate, policy.evaluate(ApiKeys.PRODUCE, RequestSummary(), error, BackoffContext("connection-1", 1000L)).action)
    assertEquals(BackoffAction.Delayed, policy.evaluate(ApiKeys.PRODUCE, RequestSummary(), error, BackoffContext("connection-1", 1001L)).action)
    assertEquals(BackoffAction.Immediate, policy.evaluate(ApiKeys.PRODUCE, RequestSummary(), valid, BackoffContext("connection-1", 1002L)).action)
    assertEquals(BackoffAction.Immediate, policy.evaluate(ApiKeys.PRODUCE, RequestSummary(), error, BackoffContext("connection-1", 1003L)).action)
  }

  @Test
  def testDelayableTransientSecondFailureDelays(): Unit = {
    val policy = newPolicy()
    val response = ResponseSummary(Seq(ResourceResult("topic-0", valid = false, delayableTransient = true, protective = true)))

    assertEquals(BackoffAction.Immediate, policy.evaluate(ApiKeys.PRODUCE, RequestSummary(), response, BackoffContext("connection-1", 1000L)).action)
    val decision = policy.evaluate(ApiKeys.PRODUCE, RequestSummary(), response, BackoffContext("connection-1", 1001L))
    assertEquals(BackoffAction.Delayed, decision.action)
    assertEquals(1000L, decision.delayMs)
    assertTrue(decision.reason.contains("delayable-transient"))
  }

  @Test
  def testProtectiveSixthFailureDelays(): Unit = {
    val policy = newPolicy()
    val response = ResponseSummary(Seq(ResourceResult("topic-0", valid = false, delayableTransient = false, protective = true)))

    for (i <- 0 until 5) {
      assertEquals(BackoffAction.Immediate, policy.evaluate(ApiKeys.PRODUCE, RequestSummary(), response, BackoffContext("connection-1", 1000L + i)).action)
    }
    val decision = policy.evaluate(ApiKeys.PRODUCE, RequestSummary(), response, BackoffContext("connection-1", 1005L))
    assertEquals(BackoffAction.Delayed, decision.action)
    assertTrue(decision.reason.contains("protective-error"))
  }

  @Test
  def testBatchAggregatesDelayedResources(): Unit = {
    val policy = newPolicy(new RetryStormBackoffConfig(true, 250L))
    val first = ResponseSummary(Seq(ResourceResult("topic-0", valid = false, delayableTransient = true, protective = true)))
    policy.evaluate(ApiKeys.PRODUCE, RequestSummary(), first, BackoffContext("connection-1", 1000L))

    val batch = ResponseSummary(Seq(
      ResourceResult("topic-0", valid = false, delayableTransient = true, protective = true),
      ResourceResult("topic-1", valid = false, delayableTransient = false, protective = true)
    ))
    val decision = policy.evaluate(ApiKeys.PRODUCE, RequestSummary(), batch, BackoffContext("connection-1", 1001L))
    assertEquals(BackoffAction.Delayed, decision.action)
    assertEquals(250L, decision.delayMs)
    assertTrue(decision.reason.contains("delayable-transient"))
  }

  @Test
  def testResponseDelayCapLimitsDecisionDelay(): Unit = {
    val policy = newPolicy(new RetryStormBackoffConfig(true, 1000L))
    val response = ResponseSummary(
      Seq(ResourceResult("topic-0", valid = false, delayableTransient = true, protective = true)),
      delayCapMs = Some(25L)
    )

    assertEquals(BackoffAction.Immediate, policy.evaluate(ApiKeys.FETCH, RequestSummary(), response, BackoffContext("connection-1", 1000L)).action)
    val decision = policy.evaluate(ApiKeys.FETCH, RequestSummary(), response, BackoffContext("connection-1", 1001L))
    assertEquals(BackoffAction.Delayed, decision.action)
    assertEquals(25L, decision.delayMs)
  }

  @Test
  def testZeroResponseDelayCapUpdatesStateButReturnsImmediate(): Unit = {
    val policy = newPolicy(new RetryStormBackoffConfig(true, 1000L))
    val capped = ResponseSummary(
      Seq(ResourceResult("topic-0", valid = false, delayableTransient = true, protective = true)),
      delayCapMs = Some(0L)
    )
    val uncapped = ResponseSummary(Seq(ResourceResult("topic-0", valid = false, delayableTransient = true, protective = true)))

    assertEquals(BackoffAction.Immediate, policy.evaluate(ApiKeys.FETCH, RequestSummary(), capped, BackoffContext("connection-1", 1000L)).action)
    assertEquals(BackoffAction.Immediate, policy.evaluate(ApiKeys.FETCH, RequestSummary(), capped, BackoffContext("connection-1", 1001L)).action)
    val decision = policy.evaluate(ApiKeys.FETCH, RequestSummary(), uncapped, BackoffContext("connection-1", 1002L))
    assertEquals(BackoffAction.Delayed, decision.action)
    assertEquals(1000L, decision.delayMs)
  }

  private def newPolicy(): RetryStormBackoffPolicy = {
    newPolicy(new RetryStormBackoffConfig(true, 1000L))
  }

  private def newPolicy(config: RetryStormBackoffConfig): RetryStormBackoffPolicy = {
    new RetryStormBackoffPolicy(config, new RetryStormBackoffStateStore())
  }
}
