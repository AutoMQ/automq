/*
 * Copyright 2024, AutoMQ CO.,LTD.
 *
 * Use of this software is governed by the Business Source License
 * included in the file BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */

package unit.kafka.controller

import kafka.controller.{BrokerChange, QueuedEvent, TopicChange}
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Test

import java.util.concurrent.PriorityBlockingQueue

class EventPriorityTest {

  @Test
  def testEventPriorityOrder(): Unit = {
    val queue = new PriorityBlockingQueue[QueuedEvent]
    val lowPriorityEvent = new QueuedEvent(TopicChange, 0)
    val highPriorityEvent = new QueuedEvent(BrokerChange, 0)

    queue.put(lowPriorityEvent)
    queue.put(highPriorityEvent)

    assertEquals(highPriorityEvent, queue.poll())
    assertEquals(lowPriorityEvent, queue.poll())
  }

}
