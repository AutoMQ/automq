/*
 * Copyright 2024, AutoMQ HK Limited.
 *
 * Use of this software is governed by the Business Source License
 * included in the file BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */

package kafka.log.streamaspect

sealed trait RawKafkaMeta

case object CleanerOffsetCheckpoint extends RawKafkaMeta

case object LogStartOffsetCheckpoint extends RawKafkaMeta

case object RecoveryPointCheckpoint extends RawKafkaMeta

case object ReplicationOffsetCheckpoint extends RawKafkaMeta
