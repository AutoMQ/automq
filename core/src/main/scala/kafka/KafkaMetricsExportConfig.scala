/*
 * Copyright 2025, AutoMQ HK Limited.
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

package kafka

import com.automq.opentelemetry.exporter.MetricsExportConfig
import com.automq.stream.s3.operator.{ObjectStorage, ObjectStorageFactory}
import kafka.server.{KafkaConfig, KafkaRaftServer, KafkaServer}
import org.apache.commons.lang3.tuple.Pair

import java.util

class KafkaMetricsExportConfig(
                          config: KafkaConfig,
                          kafkaServer: KafkaServer,
                          kafkaRaftServer: KafkaRaftServer
                        ) extends MetricsExportConfig {

  private val _objectStorage = if (config.automq.opsBuckets().isEmpty) {
    null
  } else {
    ObjectStorageFactory.instance().builder(config.automq.opsBuckets().get(0)).threadPrefix("s3-metrics").build()
  }

  override def clusterId(): String = {
    if (kafkaServer != null) {
      kafkaServer.clusterId
    } else {
      kafkaRaftServer.getSharedServer().clusterId
    }
  }

  override def isLeader: Boolean = {
    if (kafkaServer != null) {
      // For broker mode, typically only one node should upload metrics
      // You can implement your own leader selection logic here
      false
    } else {
      // For KRaft mode, only active controller uploads metrics
      kafkaRaftServer.controller.exists(controller => controller.controller != null && controller.controller.isActive)
    }
  }

  override def nodeId(): Int = config.nodeId

  override def objectStorage(): ObjectStorage = {
    _objectStorage
  }

  override def baseLabels(): util.List[Pair[String, String]] = {
    config.automq.baseLabels()
  }

  override def intervalMs(): Int = config.s3ExporterReportIntervalMs
}
