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

package kafka.controller

import scala.collection.Seq

sealed abstract class ControllerState {

  def value: Byte

  def rateAndTimeMetricName: Option[String] =
    if (hasRateAndTimeMetric) Some(s"${toString}RateAndTimeMs") else None

  protected def hasRateAndTimeMetric: Boolean = true
}

object ControllerState {

  // Note: `rateAndTimeMetricName` is based on the case object name by default. Changing a name is a breaking change
  // unless `rateAndTimeMetricName` is overridden.

  case object Idle extends ControllerState {
    def value = 0
    override protected def hasRateAndTimeMetric: Boolean = false
  }

  case object ControllerChange extends ControllerState {
    def value = 1
  }

  case object BrokerChange extends ControllerState {
    def value = 2
    // The LeaderElectionRateAndTimeMs metric existed before `ControllerState` was introduced and we keep the name
    // for backwards compatibility. The alternative would be to have the same metric under two different names.
    override def rateAndTimeMetricName = Some("LeaderElectionRateAndTimeMs")
  }

  case object TopicChange extends ControllerState {
    def value = 3
  }

  case object TopicDeletion extends ControllerState {
    def value = 4
  }

  case object AlterPartitionReassignment extends ControllerState {
    def value = 5

    override def rateAndTimeMetricName: Option[String] = Some("PartitionReassignmentRateAndTimeMs")
  }

  case object AutoLeaderBalance extends ControllerState {
    def value = 6
  }

  case object ManualLeaderBalance extends ControllerState {
    def value = 7
  }

  case object ControlledShutdown extends ControllerState {
    def value = 8
  }

  case object IsrChange extends ControllerState {
    def value = 9
  }

  case object LeaderAndIsrResponseReceived extends ControllerState {
    def value = 10
  }

  case object LogDirChange extends ControllerState {
    def value = 11
  }

  case object ControllerShutdown extends ControllerState {
    def value = 12
  }

  case object UncleanLeaderElectionEnable extends ControllerState {
    def value = 13
  }

  case object TopicUncleanLeaderElectionEnable extends ControllerState {
    def value = 14
  }

  case object ListPartitionReassignment extends ControllerState {
    def value = 15
  }

  val values: Seq[ControllerState] = Seq(Idle, ControllerChange, BrokerChange, TopicChange, TopicDeletion,
    AlterPartitionReassignment, AutoLeaderBalance, ManualLeaderBalance, ControlledShutdown, IsrChange, LeaderAndIsrResponseReceived,
    LogDirChange, ControllerShutdown, UncleanLeaderElectionEnable, TopicUncleanLeaderElectionEnable, ListPartitionReassignment)
}
