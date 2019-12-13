/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
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


import java.net.InetAddress
import java.util
import java.util.Collections
import java.util.concurrent.{DelayQueue, TimeUnit}

import kafka.network.RequestChannel
import kafka.network.RequestChannel.{EndThrottlingResponse, Response, StartThrottlingResponse}
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.memory.MemoryPool
import org.apache.kafka.common.metrics.MetricConfig
import org.apache.kafka.common.network.ClientInformation
import org.apache.kafka.common.network.ListenerName
import org.apache.kafka.common.requests.FetchRequest.PartitionData
import org.apache.kafka.common.requests.{AbstractRequest, FetchRequest, RequestContext, RequestHeader}
import org.apache.kafka.common.security.auth.{KafkaPrincipal, SecurityProtocol}
import org.apache.kafka.common.utils.MockTime
import org.easymock.EasyMock
import org.junit.{Assert, Before, Test}

class ThrottledChannelExpirationTest {
  private val time = new MockTime
  private var numCallbacksForStartThrottling: Int = 0
  private var numCallbacksForEndThrottling: Int = 0
  private val metrics = new org.apache.kafka.common.metrics.Metrics(new MetricConfig(),
                                                                    Collections.emptyList(),
                                                                    time)
  private val request = buildRequest(FetchRequest.Builder.forConsumer(0, 1000, new util.HashMap[TopicPartition, PartitionData]))._2

  private def buildRequest[T <: AbstractRequest](builder: AbstractRequest.Builder[T],
                                                 listenerName: ListenerName = ListenerName.forSecurityProtocol(SecurityProtocol.PLAINTEXT)): (T, RequestChannel.Request) = {

    val request = builder.build()
    val buffer = request.serialize(new RequestHeader(builder.apiKey, request.version, "", 0))
    val requestChannelMetrics: RequestChannel.Metrics = EasyMock.createNiceMock(classOf[RequestChannel.Metrics])

    // read the header from the buffer first so that the body can be read next from the Request constructor
    val header = RequestHeader.parse(buffer)
    val context = new RequestContext(header, "1", InetAddress.getLocalHost, KafkaPrincipal.ANONYMOUS,
      listenerName, SecurityProtocol.PLAINTEXT, ClientInformation.EMPTY)
    (request, new RequestChannel.Request(processor = 1, context = context, startTimeNanos =  0, MemoryPool.NONE, buffer,
      requestChannelMetrics))
  }

  def callback(response: Response): Unit = {
    response match {
      case _: StartThrottlingResponse => numCallbacksForStartThrottling += 1
      case _: EndThrottlingResponse => numCallbacksForEndThrottling += 1
    }
  }

  @Before
  def beforeMethod(): Unit = {
    numCallbacksForStartThrottling = 0
    numCallbacksForEndThrottling = 0
  }

  @Test
  def testCallbackInvocationAfterExpiration(): Unit = {
    val clientMetrics = new ClientQuotaManager(ClientQuotaManagerConfig(), metrics, QuotaType.Produce, time, "")

    val delayQueue = new DelayQueue[ThrottledChannel]()
    val reaper = new clientMetrics.ThrottledChannelReaper(delayQueue, "")
    try {
      // Add 4 elements to the queue out of order. Add 2 elements with the same expire timestamp.
      val channel1 = new ThrottledChannel(request, time, 10, callback)
      val channel2 = new ThrottledChannel(request, time, 30, callback)
      val channel3 = new ThrottledChannel(request, time, 30, callback)
      val channel4 = new ThrottledChannel(request, time, 20, callback)
      delayQueue.add(channel1)
      delayQueue.add(channel2)
      delayQueue.add(channel3)
      delayQueue.add(channel4)
      Assert.assertEquals(4, numCallbacksForStartThrottling)

      for(itr <- 1 to 3) {
        time.sleep(10)
        reaper.doWork()
        Assert.assertEquals(itr, numCallbacksForEndThrottling)
      }
      reaper.doWork()
      Assert.assertEquals(4, numCallbacksForEndThrottling)
      Assert.assertEquals(0, delayQueue.size())
      reaper.doWork()
      Assert.assertEquals(4, numCallbacksForEndThrottling)
    } finally {
      clientMetrics.shutdown()
    }
  }

  @Test
  def testThrottledChannelDelay(): Unit = {
    val t1: ThrottledChannel = new ThrottledChannel(request, time, 10, callback)
    val t2: ThrottledChannel = new ThrottledChannel(request, time, 20, callback)
    val t3: ThrottledChannel = new ThrottledChannel(request, time, 20, callback)
    Assert.assertEquals(10, t1.throttleTimeMs)
    Assert.assertEquals(20, t2.throttleTimeMs)
    Assert.assertEquals(20, t3.throttleTimeMs)

    for(itr <- 0 to 2) {
      Assert.assertEquals(10 - 10*itr, t1.getDelay(TimeUnit.MILLISECONDS))
      Assert.assertEquals(20 - 10*itr, t2.getDelay(TimeUnit.MILLISECONDS))
      Assert.assertEquals(20 - 10*itr, t3.getDelay(TimeUnit.MILLISECONDS))
      time.sleep(10)
    }
  }
}
