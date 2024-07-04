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

package kafka

import com.automq.shell.log.S3LogConfig
import com.automq.stream.s3.operator.BucketURI
import kafka.server.{KafkaConfig, KafkaRaftServer, KafkaServer}

class KafkaS3LogConfig(
  config: KafkaConfig,
  kafkaServer: KafkaServer,
  kafkaRaftServer: KafkaRaftServer
) extends S3LogConfig {

  override def isEnabled: Boolean = config.s3OpsTelemetryEnabled

  override def isActiveController: Boolean = {

    if (kafkaServer != null) {
      false
    } else {
      kafkaRaftServer.controller.exists(controller => controller.controller != null && controller.controller.isActive)
    }
  }

  override def clusterId(): String = {
    if (kafkaServer != null) {
      kafkaServer.clusterId
    } else {
      kafkaRaftServer.getSharedServer().clusterId
    }
  }

  override def nodeId(): Int = config.nodeId

  override def bucket(): BucketURI = {
    val buckets = config.automq.opsBuckets();
    if (buckets.isEmpty) {
      null
    } else {
      buckets.get(0)
    }
  }

}
