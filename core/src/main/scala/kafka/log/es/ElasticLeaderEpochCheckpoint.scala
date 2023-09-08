/*
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


package kafka.log.es

import kafka.server.checkpoints.LeaderEpochCheckpoint
import kafka.server.epoch.EpochEntry

import scala.jdk.CollectionConverters.{ListHasAsScala, SeqHasAsJava}

class ElasticLeaderEpochCheckpoint(val meta: ElasticLeaderEpochCheckpointMeta, val saveFunc: ElasticLeaderEpochCheckpointMeta => Unit) extends LeaderEpochCheckpoint {
  override def write(epochs: Iterable[EpochEntry]): Unit = this.synchronized {
    meta.setEntries(epochs.toList.asJava)
    saveFunc(meta)
  }

  override def read(): collection.Seq[EpochEntry] = this.synchronized {
    meta.entries().asScala.toSeq
  }
}
