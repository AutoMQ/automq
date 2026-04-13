package unit.kafka.server.streamaspect

import kafka.server.FetchSession
import kafka.server.CachedPartition
import kafka.server.streamaspect.{AsyncFetchListener, FetchListener}
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.Uuid
import org.junit.jupiter.api.Assertions.{assertEquals, assertTrue}
import org.junit.jupiter.api.Test
import org.mockito.Mockito.{mock, timeout, verify}

import java.util.concurrent.ConcurrentLinkedQueue
import java.util.concurrent.{AbstractExecutorService, CountDownLatch, TimeUnit}

class AsyncFetchListenerTest {

  @Test
  def testFetchListenerDefaultBatchDelegatesPerPartition(): Unit = {
    val closedPartitions = new ConcurrentLinkedQueue[TopicPartition]()
    val delegate = new FetchListener {
      override def onFetch(topicPartition: TopicPartition, sessionId: Int, fetchOffset: Long, timestamp: Long): Unit = {}

      override def onSessionClosed(topicPartition: TopicPartition, sessionId: Int): Unit = {
        assertEquals(12, sessionId)
        closedPartitions.add(topicPartition)
      }
    }
    val partitions = new FetchSession.CACHE_MAP(2)
    partitions.add(new CachedPartition("foo", Uuid.randomUuid(), 0))
    partitions.add(new CachedPartition("foo", Uuid.randomUuid(), 1))

    delegate.onSessionClosedBatch(12, partitions)

    assertTrue(closedPartitions.contains(new TopicPartition("foo", 0)))
    assertTrue(closedPartitions.contains(new TopicPartition("foo", 1)))
    assertEquals(2, closedPartitions.size())
  }

  @Test
  def testAsyncFetchListenerBatchSubmitsSingleAsyncTask(): Unit = {
    val delegate = mock(classOf[FetchListener])
    val executor = new CountingExecutorService
    val listener = new AsyncFetchListener(delegate, executor)
    val partitions = new FetchSession.CACHE_MAP(2)
    partitions.add(new CachedPartition("foo", Uuid.randomUuid(), 0))
    partitions.add(new CachedPartition("foo", Uuid.randomUuid(), 1))

    listener.onSessionClosedBatch(34, partitions)

    assertTrue(executor.awaitSubmission(5, TimeUnit.SECONDS))
    verify(delegate, timeout(1000)).onSessionClosed(new TopicPartition("foo", 0), 34)
    verify(delegate, timeout(1000)).onSessionClosed(new TopicPartition("foo", 1), 34)
    assertTrue(executor.submissionCount == 1)
    executor.shutdown()
  }
}

private class CountingExecutorService extends AbstractExecutorService {
  @volatile var submissionCount = 0
  @volatile private var shutdownState = false
  private val submitted = new CountDownLatch(1)

  override def shutdown(): Unit = {
    shutdownState = true
  }

  override def shutdownNow(): java.util.List[Runnable] = {
    shutdownState = true
    java.util.Collections.emptyList()
  }

  override def isShutdown: Boolean = shutdownState

  override def isTerminated: Boolean = shutdownState

  override def awaitTermination(timeout: Long, unit: TimeUnit): Boolean = shutdownState

  override def execute(command: Runnable): Unit = {
    submissionCount += 1
    val thread = new Thread(() => {
      try command.run()
      finally submitted.countDown()
    })
    thread.setDaemon(true)
    thread.start()
  }

  def awaitSubmission(timeout: Long, unit: TimeUnit): Boolean = submitted.await(timeout, unit)
}
