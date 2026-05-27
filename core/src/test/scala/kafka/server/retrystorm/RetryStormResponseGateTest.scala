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
import org.junit.jupiter.api.Tag
import org.junit.jupiter.api.Test

import java.util.concurrent.atomic.AtomicInteger

@Tag("S3Unit")
class RetryStormResponseGateTest {

  @Test
  def testImmediatePathSendsNow(): Unit = {
    val sends = new AtomicInteger(0)
    val scheduler = new CapturingScheduler
    val gate = newGate(
      ResponseSummary(Seq(ResourceResult("topic-0", valid = true, delayableTransient = false, protective = false))),
      scheduler,
      RetryStormBackoffLogger.Noop
    )

    gate.sendOrDelay(RetryStormRequestContext(ApiKeys.PRODUCE, "connection-1", 1000L), "request", "response", () => sends.incrementAndGet())

    assertEquals(1, sends.get())
    assertEquals(0, scheduler.scheduled.get())
  }

  @Test
  def testDelayedPathSchedulesAndLogs(): Unit = {
    val sends = new AtomicInteger(0)
    val scheduler = new CapturingScheduler
    val logger = new CapturingLogger
    val summary = ResponseSummary(Seq(ResourceResult("topic-0", valid = false, delayableTransient = true, protective = true)))
    val gate = newGate(summary, scheduler, logger)
    val context = RetryStormRequestContext(ApiKeys.PRODUCE, "connection-1", 1000L)

    gate.sendOrDelay(context, "request", "response", () => sends.incrementAndGet())
    gate.sendOrDelay(context.copy(nowMs = 1001L), "request", "response", () => sends.incrementAndGet())

    assertEquals(1, sends.get())
    assertEquals(1, scheduler.scheduled.get())
    assertEquals(1, logger.logged.get())
  }

  private def newGate(summary: ResponseSummary,
                      scheduler: CapturingScheduler,
                      logger: RetryStormBackoffLogger): RetryStormResponseGate = {
    val policy = new RetryStormBackoffPolicy(
      new RetryStormBackoffConfig(true, 1000L),
      new RetryStormBackoffStateStore()
    )
    val extractors: Map[ApiKeys, (AnyRef, AnyRef) => ResponseSummary] =
      Map(ApiKeys.PRODUCE -> ((_: AnyRef, _: AnyRef) => summary))
    new RetryStormResponseGate(policy, scheduler, logger, extractors)
  }

  private class CapturingScheduler extends RetryStormDelayedResponseScheduler(10L) {
    val scheduled = new AtomicInteger(0)

    override def schedule(request: AnyRef, response: AnyRef, delayMs: Long, reason: String, sendNow: () => Unit): Unit = {
      scheduled.incrementAndGet()
    }
  }

  private class CapturingLogger extends RetryStormBackoffLogger {
    val logged = new AtomicInteger(0)

    override def logDelayed(context: RetryStormRequestContext, responseSummary: ResponseSummary, decision: BackoffDecision): Unit = {
      logged.incrementAndGet()
    }
  }
}
