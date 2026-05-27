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

import io.netty.util.{HashedWheelTimer, Timeout}

import java.util.concurrent.{ConcurrentHashMap, TimeUnit}
import java.util.concurrent.atomic.AtomicBoolean

/**
 * Schedules final response send callbacks for retry storm delayed responses.
 */
class RetryStormDelayedResponseScheduler(tickMs: Long = 100L) {
  private val timer = new HashedWheelTimer(tickMs, TimeUnit.MILLISECONDS)
  private val pending = new ConcurrentHashMap[Timeout, ScheduledSend]()
  private val closed = new AtomicBoolean(false)

  def schedule(request: AnyRef, response: AnyRef, delayMs: Long, reason: String, sendNow: () => Unit): Unit = {
    val _ = (request, response, reason)
    if (delayMs <= 0 || closed.get()) {
      sendNow()
      return
    }

    val roundedDelayMs = roundUpToTick(delayMs)
    val scheduledSend = new ScheduledSend(sendNow)
    try {
      val timeout = timer.newTimeout(timeout => {
        val scheduled = pending.remove(timeout)
        if (scheduled != null) {
          scheduled.send()
        }
      }, roundedDelayMs, TimeUnit.MILLISECONDS)
      if (closed.get()) {
        timeout.cancel()
        scheduledSend.send()
      } else {
        pending.put(timeout, scheduledSend)
      }
    } catch {
      case _: IllegalStateException =>
        scheduledSend.send()
    }
  }

  def shutdown(): Unit = {
    closed.set(true)
    val iterator = pending.entrySet().iterator()
    while (iterator.hasNext) {
      val entry = iterator.next()
      iterator.remove()
      entry.getKey.cancel()
      entry.getValue.send()
    }
    timer.stop()
  }

  private def roundUpToTick(delayMs: Long): Long = {
    if (delayMs <= tickMs) {
      tickMs
    } else {
      ((delayMs + tickMs - 1) / tickMs) * tickMs
    }
  }

  private class ScheduledSend(sendNow: () => Unit) {
    private val sent = new AtomicBoolean(false)

    def send(): Unit = {
      if (sent.compareAndSet(false, true)) {
        sendNow()
      }
    }
  }
}
