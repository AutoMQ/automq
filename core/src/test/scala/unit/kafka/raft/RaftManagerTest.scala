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
package kafka.raft

import java.nio.channels.FileChannel
import java.nio.channels.OverlappingFileLockException
import java.nio.file.Path
import java.nio.file.StandardOpenOption
import java.util.Properties
import java.util.concurrent.CompletableFuture
import kafka.log.LogManager
import kafka.raft.KafkaRaftManager.RaftIoThread
import kafka.server.KafkaConfig
import kafka.server.KafkaRaftServer.BrokerRole
import kafka.server.KafkaRaftServer.ControllerRole
import kafka.server.KafkaRaftServer.ProcessRole
import kafka.server.MetaProperties
import kafka.utils.TestUtils
import kafka.tools.TestRaftServer.ByteArraySerde
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.Uuid
import org.apache.kafka.common.metrics.Metrics
import org.apache.kafka.common.utils.Time
import org.apache.kafka.raft.KafkaRaftClient
import org.apache.kafka.raft.RaftConfig
import org.junit.jupiter.api.Assertions._
import org.junit.jupiter.api.Test
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.ValueSource
import org.apache.kafka.server.fault.{FaultHandler, MockFaultHandler}
import org.mockito.Mockito._

class RaftManagerTest {
  private def createConfig(
    processRoles: Set[ProcessRole],
    nodeId: Int,
    logDir: Option[Path],
    metadataDir: Option[Path]
  ): KafkaConfig = {
    val props = new Properties
    logDir.foreach { value =>
      props.setProperty(KafkaConfig.LogDirProp, value.toString)
    }
    metadataDir.foreach { value =>
      props.setProperty(KafkaConfig.MetadataLogDirProp, value.toString)
    }
    props.setProperty(KafkaConfig.ProcessRolesProp, processRoles.mkString(","))
    props.setProperty(KafkaConfig.NodeIdProp, nodeId.toString)
    props.setProperty(KafkaConfig.ControllerListenerNamesProp, "SSL")
    if (processRoles.contains(BrokerRole)) {
      props.setProperty(KafkaConfig.InterBrokerListenerNameProp, "PLAINTEXT")
      if (processRoles.contains(ControllerRole)) { // co-located
        props.setProperty(KafkaConfig.ListenersProp, "PLAINTEXT://localhost:9092,SSL://localhost:9093")
        props.setProperty(KafkaConfig.QuorumVotersProp, s"${nodeId}@localhost:9093")
      } else { // broker-only
        val voterId = nodeId + 1
        props.setProperty(KafkaConfig.QuorumVotersProp, s"${voterId}@localhost:9093")
      }
    } else if (processRoles.contains(ControllerRole)) { // controller-only
      props.setProperty(KafkaConfig.ListenersProp, "SSL://localhost:9093")
      props.setProperty(KafkaConfig.QuorumVotersProp, s"${nodeId}@localhost:9093")
    }

    new KafkaConfig(props)
  }

  private def createRaftManager(
    topicPartition: TopicPartition,
    config: KafkaConfig
  ): KafkaRaftManager[Array[Byte]] = {
    val topicId = new Uuid(0L, 2L)
    val metaProperties = MetaProperties(
      clusterId = Uuid.randomUuid.toString,
      nodeId = config.nodeId
    )

    new KafkaRaftManager[Array[Byte]](
      metaProperties,
      config,
      new ByteArraySerde,
      topicPartition,
      topicId,
      Time.SYSTEM,
      new Metrics(Time.SYSTEM),
      Option.empty,
      CompletableFuture.completedFuture(RaftConfig.parseVoterConnections(config.quorumVoters)),
      mock(classOf[FaultHandler])
    )
  }

  @ParameterizedTest
  @ValueSource(strings = Array("broker", "controller", "broker,controller"))
  def testNodeIdPresent(processRoles: String): Unit = {
    var processRolesSet = Set.empty[ProcessRole]
    if (processRoles.contains("broker")) {
      processRolesSet = processRolesSet ++ Set(BrokerRole)
    }
    if (processRoles.contains("controller")) {
      processRolesSet = processRolesSet ++ Set(ControllerRole)
    }

    val logDir = TestUtils.tempDir()
    val nodeId = 1
    val raftManager = createRaftManager(
      new TopicPartition("__raft_id_test", 0),
      createConfig(
        processRolesSet,
        nodeId,
        Some(logDir.toPath),
        None
      )
    )
    assertEquals(nodeId, raftManager.client.nodeId.getAsInt)
    raftManager.shutdown()
  }

  @ParameterizedTest
  @ValueSource(strings = Array("metadata-only", "log-only", "both"))
  def testLogDirLockWhenControllerOnly(dirType: String): Unit = {
    val logDir = if (dirType.equals("metadata-only")) {
      None
    } else {
      Some(TestUtils.tempDir().toPath)
    }

    val metadataDir = if (dirType.equals("log-only")) {
      None
    } else {
      Some(TestUtils.tempDir().toPath)
    }

    val nodeId = 1
    val raftManager = createRaftManager(
      new TopicPartition("__raft_id_test", 0),
      createConfig(
        Set(ControllerRole),
        nodeId,
        logDir,
        metadataDir
      )
    )

    val lockPath = metadataDir.getOrElse(logDir.get).resolve(LogManager.LockFileName)
    assertTrue(fileLocked(lockPath))

    raftManager.shutdown()

    assertFalse(fileLocked(lockPath))
  }

  @Test
  def testLogDirLockWhenBrokerOnlyWithSeparateMetadataDir(): Unit = {
    val logDir = Some(TestUtils.tempDir().toPath)
    val metadataDir = Some(TestUtils.tempDir().toPath)

    val nodeId = 1
    val raftManager = createRaftManager(
      new TopicPartition("__raft_id_test", 0),
      createConfig(
        Set(BrokerRole),
        nodeId,
        logDir,
        metadataDir
      )
    )

    val lockPath = metadataDir.getOrElse(logDir.get).resolve(LogManager.LockFileName)
    assertTrue(fileLocked(lockPath))

    raftManager.shutdown()

    assertFalse(fileLocked(lockPath))
  }

  private def fileLocked(path: Path): Boolean = {
    TestUtils.resource(FileChannel.open(path, StandardOpenOption.WRITE)) { channel =>
      try {
        Option(channel.tryLock()).foreach(_.close())
        false
      } catch {
        case _: OverlappingFileLockException => true
      }
    }
  }

  @Test
  def testShutdownIoThread(): Unit = {
    val raftClient = mock(classOf[KafkaRaftClient[String]])
    val faultHandler = new MockFaultHandler("RaftManagerTestFaultHandler")
    val ioThread = new RaftIoThread(raftClient, threadNamePrefix = "test-raft", faultHandler)

    when(raftClient.isRunning).thenReturn(true)
    assertTrue(ioThread.isRunning)

    val shutdownFuture = new CompletableFuture[Void]
    when(raftClient.shutdown(5000)).thenReturn(shutdownFuture)

    ioThread.initiateShutdown()
    assertTrue(ioThread.isRunning)
    assertTrue(ioThread.isShutdownInitiated)
    verify(raftClient).shutdown(5000)

    shutdownFuture.complete(null)
    when(raftClient.isRunning).thenReturn(false)
    ioThread.run()
    assertFalse(ioThread.isRunning)
    assertTrue(ioThread.isShutdownComplete)
    assertNull(faultHandler.firstException)
  }

  @Test
  def testUncaughtExceptionInIoThread(): Unit = {
    val raftClient = mock(classOf[KafkaRaftClient[String]])
    val faultHandler = new MockFaultHandler("RaftManagerTestFaultHandler")
    val ioThread = new RaftIoThread(raftClient, threadNamePrefix = "test-raft", faultHandler)

    when(raftClient.isRunning).thenReturn(true)
    assertTrue(ioThread.isRunning)

    val exception = new RuntimeException()
    when(raftClient.poll()).thenThrow(exception)
    ioThread.run()

    assertTrue(ioThread.isShutdownComplete)
    assertTrue(ioThread.isThreadFailed)
    assertFalse(ioThread.isRunning)

    val caughtException = faultHandler.firstException.getCause
    assertEquals(exception, caughtException)
  }
}
