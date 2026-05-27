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

import org.junit.jupiter.api.Assertions._
import org.junit.jupiter.api.Test

import java.util.concurrent.atomic.AtomicInteger

class RetryStormDelayedResponseSchedulerTest {

  @Test
  def testDelayMsZeroSendsImmediately(): Unit = {
    val scheduler = new RetryStormDelayedResponseScheduler(10L)
    val sends = new AtomicInteger(0)
    try {
      scheduler.schedule(null, null, 0L, "none", () => sends.incrementAndGet())
      assertEquals(1, sends.get())
    } finally {
      scheduler.shutdown()
    }
  }

  @Test
  def testPositiveDelaySendsLater(): Unit = {
    val scheduler = new RetryStormDelayedResponseScheduler(10L)
    val sends = new AtomicInteger(0)
    try {
      scheduler.schedule(null, null, 30L, "delayable-transient", () => sends.incrementAndGet())
      assertEquals(0, sends.get())
      eventually(1000L)(assertEquals(1, sends.get()))
    } finally {
      scheduler.shutdown()
    }
  }

  @Test
  def testShutdownSendsPendingResponsesOnce(): Unit = {
    val scheduler = new RetryStormDelayedResponseScheduler(1000L)
    val sends = new AtomicInteger(0)
    scheduler.schedule(null, null, 10000L, "protective-error", () => sends.incrementAndGet())

    scheduler.shutdown()
    assertEquals(1, sends.get())

    Thread.sleep(50L)
    assertEquals(1, sends.get())
  }

  private def eventually(timeoutMs: Long)(assertion: => Unit): Unit = {
    val deadline = System.currentTimeMillis() + timeoutMs
    var lastError: AssertionError = null
    while (System.currentTimeMillis() < deadline) {
      try {
        assertion
        return
      } catch {
        case e: AssertionError =>
          lastError = e
          Thread.sleep(10L)
      }
    }
    if (lastError != null) throw lastError
  }
}
