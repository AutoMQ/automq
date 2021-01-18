/**
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
package kafka.utils

import java.io.ByteArrayOutputStream
import java.util.Collections

import org.apache.kafka.common.MetricName
import org.apache.kafka.common.metrics.Metrics
import org.apache.kafka.common.metrics.internals.IntGaugeSuite
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Test
import org.slf4j.LoggerFactory

import scala.jdk.CollectionConverters._

class ToolsUtilsTest {
  private val log = LoggerFactory.getLogger(classOf[ToolsUtilsTest])

  @Test def testIntegerMetric(): Unit = {
    val outContent = new ByteArrayOutputStream()
    val metrics = new Metrics
    val suite = new IntGaugeSuite[String](log, "example", metrics, (k: String) => new MetricName(k + "-bar", "test", "A test metric", Collections.singletonMap("key", "value")), 1)
    suite.increment("foo")
    Console.withOut(outContent) {
      ToolsUtils.printMetrics(metrics.metrics.asScala)
      assertTrue(outContent.toString.split("\n").exists(line => line.trim.matches("^test:foo-bar:\\{key=value\\}     : 1$")))
    }
  }

}
