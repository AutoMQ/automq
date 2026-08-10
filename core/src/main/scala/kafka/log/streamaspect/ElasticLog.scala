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

package kafka.log.streamaspect

import com.automq.stream.api.{Client, CreateStreamOptions, KeyValue, OpenStreamOptions, Stream}
import com.automq.stream.s3.metrics.{Metrics, MetricsLevel}
import com.automq.stream.utils.{AsyncLogger, FutureUtil, Systems, Threads}
import com.automq.stream.utils.threads.EventLoop
import com.typesafe.scalalogging.Logger
import io.opentelemetry.api.common.Attributes
import io.netty.buffer.Unpooled
import kafka.automq.runtime.{DataPathMonitor, ElasticFailureHandlers}
import kafka.cluster.PartitionSnapshot
import kafka.log.LocalLog.CleanedFileSuffix
import kafka.log._
import kafka.log.streamaspect.ElasticLogFileRecords.{BatchIteratorRecordsAdaptor, PooledMemoryRecords}
import kafka.log.streamaspect.reassignment.FastPartitionReassignmentManager
import kafka.metrics.KafkaMetricsUtil
import kafka.utils.Logging
import org.apache.kafka.common.errors.s3.StreamFencedException
import org.apache.kafka.common.errors.{KafkaStorageException, OffsetOutOfRangeException}
import org.apache.kafka.common.message.FetchResponseData
import org.apache.kafka.common.record.MemoryRecords
import org.apache.kafka.common.utils.{ThreadUtils, Time}
import org.apache.kafka.common.{KafkaException, TopicPartition, Uuid}
import org.apache.kafka.metadata.stream.StreamTags
import org.apache.kafka.server.metrics.KafkaMetricsGroup
import org.apache.kafka.server.util.Scheduler
import org.apache.kafka.storage.internals.checkpoint.LeaderEpochCheckpointFile
import org.apache.kafka.storage.internals.log._
import org.slf4j.LoggerFactory

import java.io.{File, IOException}
import java.nio.ByteBuffer
import java.nio.file.Files
import java.util
import java.util.{Collections, Optional}
import java.util.concurrent._
import java.util.concurrent.atomic.{AtomicLong, AtomicReference}
import scala.collection.mutable.ListBuffer
import scala.jdk.CollectionConverters._
import scala.util.control.Breaks.{break, breakable}

/**
 * An append-only log for storing messages in elastic stream. The log is a sequence of LogSegments, each with a base offset.
 * New log segments are created according to a configurable policy that controls the size in bytes or time interval
 * for a given segment.
 *
 * NOTE: this class is not thread-safe, and it relies on the thread safety provided by the Log class.
 *
 * NOTE2: If you just want to pass an initial value to super class's variable field,
 * it is better to use another variable to hold the initial value.
 * See https://stackoverflow.com/questions/6656868/why-cant-i-assign-to-var-in-scala-subclass?rq=3.
 *
 * @param metaStream                The meta stream
 * @param streamManager             The stream manager
 * @param streamSliceManager        The stream slice manager
 * @param producerStateManager      The producer state manager
 * @param logSegmentManager         The log segment manager.
 * @param partitionMeta             The partition meta
 * @param leaderEpochCheckpointMeta The leader epoch checkpoint meta
 * @param __dir                     The directory in which log segments are created.
 * @param _config                   The log configuration settings.
 * @param segments                  The non-empty log segments recovered from disk
 * @param _nextOffsetMetadata       The offset where the next message could be appended.
 * @param scheduler                 The thread pool scheduler used for background actions
 * @param time                      The time instance used for checking the clock
 * @param topicPartition            The topic partition associated with this log
 * @param logDirFailureChannel      The LogDirFailureChannel instance to asynchronously handle Log dir failure
 * @param _initStartOffset          The start offset of the log. Only used for log-startOffset initialization
 */
class ElasticLog(val metaStream: MetaStream,
    val streamManager: ElasticLogStreamManager,
    val streamSliceManager: ElasticStreamSliceManager,
    val producerStateManager: ProducerStateManager,
    val logSegmentManager: ElasticLogSegmentManager,
    val partitionMeta: ElasticPartitionMeta,
    val leaderEpochCheckpointMeta: ElasticLeaderEpochCheckpointMeta,
    __dir: File,
    _config: LogConfig,
    segments: LogSegments,
    _nextOffsetMetadata: LogOffsetMetadata,
    scheduler: Scheduler,
    time: Time,
    topicPartition: TopicPartition,
    logDirFailureChannel: LogDirFailureChannel,
    val _initStartOffset: Long = 0,
    leaderEpoch: Long,
    snapshotRead: Boolean = false
) extends LocalLog(__dir, _config, segments, partitionMeta.getRecoverOffset, _nextOffsetMetadata, scheduler, time, topicPartition, logDirFailureChannel) {

    import ElasticLog._

    override protected lazy val logger: Logger =
        Logger(AsyncLogger.wrap(LoggerFactory.getLogger(loggerName)))

    this.logIdent = s"[ElasticLog partition=$topicPartition epoch=$leaderEpoch] "
    /**
     * The next valid offset. The records with offset smaller than $confirmOffset has been confirmed by ElasticStream.
     */
    private val _confirmOffset: AtomicReference[LogOffsetMetadata] = new AtomicReference(_nextOffsetMetadata)
    var confirmOffsetChangeListener: Option[() => Unit] = None

    private val appendAckQueue = new LinkedBlockingQueue[Long]()
    val appendAckThread = APPEND_CALLBACK_EXECUTOR(math.abs(logIdent.hashCode % APPEND_CALLBACK_EXECUTOR.length))
    @volatile private[log] var lastAppendAckFuture: CompletableFuture[Void] = CompletableFuture.completedFuture(null)

    private val readAsyncThread = READ_ASYNC_EXECUTOR(math.abs(logIdent.hashCode % READ_ASYNC_EXECUTOR.length))
    var logStartOffset = _initStartOffset

    // persist log meta when lazy stream real create
    streamManager.setListener((_, event) => {
        if (event == ElasticStreamMetaEvent.STREAM_DO_CREATE) {
            logSegmentManager.asyncPersistLogMeta()
            logSegmentManager.notifySegmentUpdate();
        }
    })

    private def maybeHandleIOExceptionAsync[T](msg: => String)(fun: => CompletableFuture[T]): CompletableFuture[T] = {
        ElasticLog.maybeHandleIOExceptionAsync(logDirFailureChannel, _dir.getPath, msg) {
            fun
        }
    }

    private def getLogStartOffsetFromMeta: Long = partitionMeta.getStartOffset

    def persistLogStartOffset(): Unit = {
        if (getLogStartOffsetFromMeta == logStartOffset) {
            return
        }
        partitionMeta.setStartOffset(logStartOffset)
        asyncPersistPartitionMeta().join()
        info(s"saved logStartOffset: $logStartOffset")
    }

    // support reading from offsetCheckpointFile
    def getCleanerOffsetCheckpointFromMeta: Long = partitionMeta.getCleanerOffset

    def persistCleanerOffsetCheckpoint(offsetCheckpoint: Long): Unit = {
        if (getCleanerOffsetCheckpointFromMeta == offsetCheckpoint) {
            return
        }
        partitionMeta.setCleanerOffset(offsetCheckpoint)
        asyncPersistPartitionMeta().join()
        if (isDebugEnabled) {
            debug(s"saved cleanerOffsetCheckpoint: $offsetCheckpoint")
        }
    }

    def persistRecoverOffsetCheckpoint(): Unit = {
        if (partitionMeta.getRecoverOffset == recoveryPoint) {
            return
        }
        partitionMeta.setRecoverOffset(recoveryPoint)
        asyncPersistPartitionMeta().join()
        if (isDebugEnabled) {
            debug(s"saved recoverOffsetCheckpoint: $recoveryPoint")
        }
    }

    def saveLeaderEpochCheckpoint(meta: ElasticLeaderEpochCheckpointMeta): Unit = {
        persistMeta(metaStream, MetaKeyValue.of(MetaStream.LEADER_EPOCH_CHECKPOINT_KEY, ByteBuffer.wrap(meta.encode())))
    }

    def newSegment(baseOffset: Long, time: Time, suffix: String = ""): ElasticLogSegment = {
        // In roll, before new segment, last segment will be inactive by #onBecomeInactiveSegment
        val rst = createAndSaveSegment(logSegmentManager, suffix, logIdent = logIdent)(baseOffset, _dir, config, streamSliceManager, time)
        // sync await segment meta persist, cause of if not, when append and node crash, the data will be treated as the previous segment data.
        rst._2.get()
        rst._1
    }

    private def persistLogMeta(): Unit = {
        if (snapshotRead) {
            return
        }
        logSegmentManager.persistLogMeta()
    }

    private def asyncPersistPartitionMeta(): CompletableFuture[?] = {
        if (snapshotRead) {
            return CompletableFuture.completedFuture(null)
        }
        val future = metaStream.append(
            MetaKeyValue.of(MetaStream.PARTITION_META_KEY, ElasticPartitionMeta.encode(partitionMeta)))
        if (isDebugEnabled) {
            debug(s"${logIdent}save partition meta $partitionMeta")
        }
        future
    }

    override private[log] def append(lastOffset: Long, largestTimestamp: Long, offsetOfMaxTimestamp: Long,
        records: MemoryRecords): Unit = {
        val activeSegment = segments.activeSegment
        val startTimestamp = time.nanoseconds()

        val permit = records.sizeInBytes()
        if (!APPEND_PERMIT_SEMAPHORE.tryAcquire(permit)) {
            while (!APPEND_PERMIT_SEMAPHORE.tryAcquire(permit, 1, TimeUnit.SECONDS)) {
                tryAppendStatistics()
            }
            APPEND_PERMIT_ACQUIRE_FAIL_TIME_HIST.update(System.nanoTime() - startTimestamp)
        }

        try {
            activeSegment.append(lastOffset, largestTimestamp, offsetOfMaxTimestamp, records)
        } catch {
            case e: Throwable =>
                APPEND_PERMIT_SEMAPHORE.release(permit)
                recordLogWriteFailedIfUnexpected(e)
                throw e
        }

        APPEND_TIME_HIST.update(System.nanoTime() - startTimestamp)
        val endOffset = lastOffset + 1
        updateLogEndOffset(endOffset)
        val cf = activeSegment.asInstanceOf[ElasticLogSegment].asyncLogFlush()
        DataPathMonitor.recordAppendPending(cf, startTimestamp)
        cf.whenComplete((_, throwable) => {
            APPEND_PERMIT_SEMAPHORE.release(permit)
            if (throwable != null) {
                recordLogWriteFailedIfUnexpected(FutureUtil.cause(throwable))
            }
        })
        lastAppendAckFuture = cf.thenCompose[Void](_ => {
            APPEND_CALLBACK_TIME_HIST.update(System.nanoTime() - startTimestamp)
            // run callback async by executors to avoid deadlock when asyncLogFlush is called by append thread.
            // append callback executor is single thread executor, so the callback will be executed in order.
            val startNanos = System.nanoTime()
            var notify = false
            breakable {
                while (true) {
                    val offset = _confirmOffset.get()
                    if (offset.messageOffset < endOffset) {
                        _confirmOffset.compareAndSet(offset, new LogOffsetMetadata(endOffset, activeSegment.baseOffset, activeSegment.size))
                        notify = true
                    } else {
                        break()
                    }
                }
            }
            if (notify) {
                appendAckQueue.offer(endOffset)
                CompletableFuture.runAsync(new Runnable {
                    override def run(): Unit = {
                        try {
                            appendCallback(startNanos)
                        } catch {
                            case e: Throwable =>
                                error(s"append callback error", e)
                        }
                    }
                }, appendAckThread)
            } else {
                CompletableFuture.completedFuture(null)
            }
        })
    }

    private def appendCallback(startNanos: Long): Unit = {
        // group notify
        if (appendAckQueue.isEmpty) {
            return
        }
        appendAckQueue.clear()
        confirmOffsetChangeListener.foreach(_.apply())
        APPEND_ACK_TIME_HIST.update(System.nanoTime() - startNanos)

        tryAppendStatistics()
    }

    private def tryAppendStatistics(): Unit = {
        val lastRecordTimestamp = LAST_RECORD_TIMESTAMP.get()
        val now = System.currentTimeMillis()
        if (now - lastRecordTimestamp > 60000 && LAST_RECORD_TIMESTAMP.compareAndSet(lastRecordTimestamp, now)) {
            val remainingPermits = APPEND_PERMIT_SEMAPHORE.availablePermits()
            logger.info(s"log append cost, permitAcquireFail=${KafkaMetricsUtil.histToString(APPEND_PERMIT_ACQUIRE_FAIL_TIME_HIST)}, " +
                s"remainingPermit=$remainingPermits/$APPEND_PERMIT, " +
                s"append=${KafkaMetricsUtil.histToString(APPEND_TIME_HIST)}, " +
                s"callback=${KafkaMetricsUtil.histToString(APPEND_CALLBACK_TIME_HIST)}, " +
                s"ack=${KafkaMetricsUtil.histToString(APPEND_ACK_TIME_HIST)}")
            APPEND_PERMIT_ACQUIRE_FAIL_TIME_HIST.clear()
            APPEND_TIME_HIST.clear()
            APPEND_CALLBACK_TIME_HIST.clear()
            APPEND_ACK_TIME_HIST.clear()
        }
    }

    private[log] def confirmOffset: LogOffsetMetadata = {
        _confirmOffset.get()
    }

    override private[log] def flush(offset: Long): Unit = {
        val currentRecoveryPoint = recoveryPoint
        if (currentRecoveryPoint <= offset) {
            val segmentsToFlush = segments.values(currentRecoveryPoint, offset)
            segmentsToFlush.forEach(s => s.flush())
        }
    }

    /**
     * Asynchronously read messages from the log.
     *
     * @param startOffset        The offset to begin reading at
     * @param maxLength          The maximum number of bytes to read
     * @param minOneMessage      If this is true, the first message will be returned even if it exceeds `maxLength` (if one exists)
     * @param maxOffsetMetadata  The metadata of the maximum offset to be fetched
     * @param includeAbortedTxns If true, aborted transactions are included
     * @return The fetch data information including fetch starting offset metadata and messages read.
     */
    def readAsync(startOffset: Long,
        maxLength: Int,
        minOneMessage: Boolean,
        maxOffsetMetadata: LogOffsetMetadata,
        includeAbortedTxns: Boolean): CompletableFuture[FetchDataInfo] = {
        ElasticFailureHandlers.readAsync(topicPartition, startOffset, offset =>
            readAsync0(offset, maxLength, minOneMessage, maxOffsetMetadata, includeAbortedTxns)
        )
    }

    private def readAsync0(startOffset: Long,
        maxLength: Int,
        minOneMessage: Boolean,
        maxOffsetMetadata: LogOffsetMetadata,
        includeAbortedTxns: Boolean): CompletableFuture[FetchDataInfo] = {
        maybeHandleIOExceptionAsync(s"Exception while reading from $topicPartition in dir ${dir.getParent}") {
            if (isTraceEnabled) {
                trace(s"Reading maximum $maxLength bytes at offset $startOffset from log with " +
                    s"total length ${segments.sizeInBytes} bytes")
            }
          // get LEO from super class
            val endOffsetMetadata = nextOffsetMetadata
            val endOffset = endOffsetMetadata.messageOffset
            val segmentOpt = segments.lastSegment
                // firstly, check the last segment (with the largest base offset) to avoid call `floorSegment` method
                .filter(segment => segment.baseOffset <= startOffset)
                // if `startOffset` does not fall into the last segment, call `floorSegment` method to find the correct segment
                .or(() => segments.floorSegment(startOffset))

            var finalSegmentOpt: Optional[LogSegment] = Optional.empty()

            def readFromSegment(segOpt: Optional[LogSegment]): CompletableFuture[FetchDataInfo] = {
                if (segOpt.isEmpty) {
                    CompletableFuture.completedFuture(null)
                } else {
                    val segment = segOpt.get
                    val baseOffset = segment.baseOffset

                    val maxPosition =
                        // Use the max offset position if it is on this segment; otherwise, the segment size is the limit.
                        if (maxOffsetMetadata.segmentBaseOffset == segment.baseOffset) maxOffsetMetadata.relativePositionInSegment
                        else segment.size

                    segment.readAsync(startOffset, maxLength, Optional.of(maxPosition), maxOffsetMetadata.messageOffset, minOneMessage)
                        .thenCompose(dataInfo => {
                            if (dataInfo != null) {
                                finalSegmentOpt = segOpt
                                CompletableFuture.completedFuture(dataInfo)
                            } else {
                                readFromSegment(segments.higherSegment(baseOffset))
                            }
                        })
                }
            }

            // return error on attempt to read beyond the log end offset
            if (startOffset > endOffset || segmentOpt.isEmpty)
                CompletableFuture.failedFuture(new OffsetOutOfRangeException(s"Received request for offset $startOffset for partition $topicPartition, " +
                    s"but we only have log segments upto $endOffset."))
            else if (startOffset == maxOffsetMetadata.messageOffset)
                CompletableFuture.completedFuture(LocalLog.emptyFetchDataInfo(maxOffsetMetadata, includeAbortedTxns))
            else if (startOffset > maxOffsetMetadata.messageOffset)
                CompletableFuture.completedFuture(LocalLog.emptyFetchDataInfo(convertToOffsetMetadataOrThrow(startOffset), includeAbortedTxns))
            else {
                // Do the read on the segment with a base offset less than the target offset
                // but if that segment doesn't contain any messages with an offset greater than that
                // continue to read from successive segments until we get some messages or we reach the end of the log
                readFromSegment(segmentOpt).thenCompose(fetchDataInfo => {
                    if (fetchDataInfo == null) {
                        // okay we are beyond the end of the last segment with no data fetched although the start offset is in range,
                        // this can happen when all messages with offset larger than start offsets have been deleted.
                        // In this case, we will return the empty set with log end offset metadata
                        CompletableFuture.completedFuture(new FetchDataInfo(nextOffsetMetadata, MemoryRecords.EMPTY))
                    } else {
                        if (includeAbortedTxns) {
                            val upperBoundOpt = fetchDataInfo.records match {
                                case records: PooledMemoryRecords =>
                                    Some(records.lastOffset())
                                case adapter: BatchIteratorRecordsAdaptor =>
                                    Some(adapter.lastOffset())
                                case _ =>
                                    None
                            }
                            CompletableFuture.supplyAsync(() => {
                                addAbortedTransactions(startOffset, finalSegmentOpt.get, fetchDataInfo, upperBoundOpt)
                            }, readAsyncThread)
                        } else {
                            CompletableFuture.completedFuture(fetchDataInfo)
                        }
                    }
                })
            }
        }
    }

    def addAbortedTransactions(startOffset: Long,
        segment: LogSegment,
        fetchInfo: FetchDataInfo, upperBoundOffsetOpt: Option[Long]): FetchDataInfo = {
        val fetchSize = fetchInfo.records.sizeInBytes
        // A zero-byte fetch contains no record batches, so there are no aborted
        // transactions for the consumer to filter. Returning an empty aborted list also
        // avoids falling back to ElasticLogSegment.fetchUpperBoundOffset, which is not
        // implemented for elastic segments and is unnecessary for empty fetch results.
        if (fetchSize == 0) {
            return new FetchDataInfo(fetchInfo.fetchOffsetMetadata,
                fetchInfo.records,
                fetchInfo.firstEntryIncomplete,
                Optional.of(Collections.emptyList()))
        }
        val startOffsetPosition = new OffsetPosition(fetchInfo.fetchOffsetMetadata.messageOffset,
            fetchInfo.fetchOffsetMetadata.relativePositionInSegment)
        val upperBoundOffset = upperBoundOffsetOpt match {
            case Some(x) => x
            case None =>
                val opt = segment.fetchUpperBoundOffset(startOffsetPosition, fetchSize);
                if (opt.isPresent) {
                    opt.getAsLong
                } else {
                    val opt = segments.higherSegment(segment.baseOffset).map(_.baseOffset)
                    if (opt.isPresent) {
                        opt.get()
                    } else {
                        logEndOffset
                    }
                }
        }

        val abortedTransactions = ListBuffer.empty[FetchResponseData.AbortedTransaction]

        def accumulator(abortedTxns: scala.collection.Seq[AbortedTxn]): Unit = abortedTransactions ++= abortedTxns.map(_.asAbortedTransaction)

        collectAbortedTransactions(startOffset, upperBoundOffset, segment, accumulator)

        new FetchDataInfo(fetchInfo.fetchOffsetMetadata,
            fetchInfo.records,
            fetchInfo.firstEntryIncomplete,
            Optional.of(abortedTransactions.toList.asJava))
    }

    override protected def addAbortedTransactions(startOffset: Long,
        segment: LogSegment,
        fetchInfo: FetchDataInfo): FetchDataInfo = {
        addAbortedTransactions(startOffset, segment, fetchInfo, Option.empty)
    }

    /**
     * ref. LocalLog#replcaseSegments
     */
    override def replaceSegments(newSegments: collection.Seq[LogSegment],
        oldSegments: collection.Seq[LogSegment]): Iterable[LogSegment] = {
        val existingSegments = segments
        val sortedNewSegments = newSegments.sortBy(_.baseOffset)
        // Some old segments may have been removed from index and scheduled for async deletion after the caller reads segments
        // but before this method is executed. We want to filter out those segments to avoid calling deleteSegmentFiles()
        // multiple times for the same segment.
        val sortedOldSegments = oldSegments.filter(seg => existingSegments.contains(seg.baseOffset)).sortBy(_.baseOffset)

        // add new segments
        sortedNewSegments.reverse.foreach(segment => {
            existingSegments.add(segment)
            logSegmentManager.put(segment.baseOffset, segment.asInstanceOf[ElasticLogSegment])
        })
        val newSegmentBaseOffsets = sortedNewSegments.map(_.baseOffset).toSet

        // deleted but not replaced segments
        val deletedNotReplaced = sortedOldSegments.map(seg => {
            // Do not remove the segment if (seg.baseOffset == sortedNewSegments.head.baseOffset). It is actually the newly replaced segment.
            if (seg.baseOffset != sortedNewSegments.head.baseOffset) {
                existingSegments.remove(seg.baseOffset)
                logSegmentManager.remove(seg.baseOffset)
            }
            seg.close()
            if (newSegmentBaseOffsets.contains(seg.baseOffset)) Option.empty else Some(seg)
        }).filter(item => item.isDefined).map(item => item.get)

        persistLogMeta()
        deletedNotReplaced
    }

    override def createNewCleanedSegment(dir: File, logConfig: LogConfig, baseOffset: Long): LogSegment = {
        val (newSegment, newSegmentCf) = createAndSaveSegment(logSegmentManager, CleanedFileSuffix, logIdent)(baseOffset, _dir, config, streamSliceManager, time)
        newSegmentCf.get()
        newSegment
    }

    /**
     * ref. LocalLog#close
     */
    override private[log] def close(): Unit = {
        metaStream.beforeClose()
        markFlushed(logEndOffset)
        partitionMeta.setCleanedShutdown(true)
        partitionMeta.setStartOffset(logStartOffset)
        partitionMeta.setRecoverOffset(recoveryPoint)

        maybeHandleIOException(s"Error while closing $topicPartition in dir ${dir.getParent}") {
            // https://github.com/AutoMQ/automq/issues/2038
            // ElasticLogMeta should be saved after all segments are closed cause of the last segment may append new time index when close.
            // AutoMQ inject start
            val activeSegment = segments.activeSegment
            val flushFuture = try {
                checkIfMemoryMappedBufferClosed()
                segments.close()
                logSegmentManager.asyncPersistLogMeta(false)
                asyncPersistPartitionMeta()
                activeSegment.asInstanceOf[ElasticLogSegment].asyncLogFlush()
            } catch {
                case e: Throwable =>
                    warn(s"${logIdent}failed to persist final metadata; closing without handoff", e)
                    CompletableFuture.failedFuture(e)
            }
            flushFuture.handle[Boolean]((_, exception) => {
                if (exception != null) {
                    warn(s"${logIdent}failed to flush the active segment; closing without handoff",
                        FutureUtil.cause(exception))
                }
                exception == null
            }).thenCompose(fastClose => closeStreams(fastClose)).handle[Void]((_, exception) => {
                if (exception != null) {
                    warn(s"${logIdent}failed to close streams", FutureUtil.cause(exception))
                }
                info("log closed")
                null
            }).join()
            // AutoMQ inject end
        }
    }

    private[streamaspect] def closeStreams(fastClose: Boolean): CompletableFuture[Void] = {
        // Initiate every data-stream close before starting the MetaStream handoff and close. The normal RPC batching
        // and Controller processing path therefore closes data-stream ownership before the later MetaStream close
        // completes. The target starts replay and data-stream open only after observing that MetaStream close.
        val dataStreamsCloseFuture = streamManager.close()
        if (snapshotRead) {
            dataStreamsCloseFuture
        } else {
            val metaStreamCloseFuture = metaStream.close(fastClose)
            CompletableFuture.allOf(dataStreamsCloseFuture, metaStreamCloseFuture)
        }
    }

    def updateLogStartOffset(offset: Long): Unit = {
        logStartOffset = offset
    }

    override def createAndDeleteSegment(newOffset: Long,
        segmentToDelete: LogSegment,
        asyncDelete: Boolean,
        reason: SegmentDeletionReason): LogSegment = {
        if (newOffset == segmentToDelete.baseOffset)
            segmentToDelete.changeFileSuffixes("", LogFileUtils.DELETED_FILE_SUFFIX)

        // AutoMQ inject start
        val (newSegment, newSegmentCf) = createAndSaveSegment(logSegmentManager, "", logIdent)(newOffset, _dir, config, streamSliceManager, time)
        newSegmentCf.get()
        // AutoMQ inject end
        segments.add(newSegment)

        reason.logReason(List(segmentToDelete))
        if (newOffset != segmentToDelete.baseOffset)
            segments.remove(segmentToDelete.baseOffset)
        LocalLog.deleteSegmentFiles(List(segmentToDelete), asyncDelete, dir, topicPartition, config, scheduler, logDirFailureChannel, logIdent)

        newSegment
    }

    override def read(startOffset: Long, maxLength: Int, minOneMessage: Boolean,
        maxOffsetMetadata: LogOffsetMetadata,
        includeAbortedTxns: Boolean): FetchDataInfo = {
        try {
            readAsync(startOffset, maxLength, minOneMessage, maxOffsetMetadata, includeAbortedTxns).get()
        } catch {
            case e: Throwable =>
                val cause = FutureUtil.cause(e)
                if (cause.isInstanceOf[KafkaException]) {
                    throw cause
                } else {
                    throw new KafkaStorageException(s"Error while reading from $topicPartition in dir ${dir.getParent}", cause)
                }
        }
    }

    private def recordLogWriteFailedIfUnexpected(throwable: Throwable): Unit = {
        if (!isClosePathExpectedException(throwable)) {
            DataPathMonitor.recordLogWriteFailed(topicPartition)
        }
    }

    private def isClosePathExpectedException(throwable: Throwable): Boolean = {
        if (throwable == null) {
            false
        } else {
            isMemoryMappedBufferClosed ||
                throwable.isInstanceOf[StreamFencedException] ||
                throwable.getClass.getName.contains("Closed") ||
                (throwable.getMessage != null && throwable.getMessage.toLowerCase(java.util.Locale.ROOT).contains("closed"))
        }
    }

    override def roll(expectedNextOffset: Option[Long] = None): LogSegment = {
        maybeHandleIOException(s"Error while rolling log segment for $topicPartition in dir ${dir.getParent}") {
            val start = time.hiResClockMs()
            checkIfMemoryMappedBufferClosed()
            val newOffset = math.max(expectedNextOffset.getOrElse(0L), logEndOffset)
            val logFile = LogFileUtils.logFile(dir, newOffset, "")
            val activeSegment = segments.activeSegment
            if (segments.contains(newOffset)) {
                // segment with the same base offset already exists and loaded
                if (activeSegment.baseOffset == newOffset && activeSegment.size == 0) {
                    // We have seen this happen (see KAFKA-6388) after shouldRoll() returns true for an
                    // active segment of size zero because of one of the indexes is "full" (due to _maxEntries == 0).
                    warn(s"Trying to roll a new log segment with start offset $newOffset " +
                        s"=max(provided offset = $expectedNextOffset, LEO = $logEndOffset) while it already " +
                        s"exists and is active with size 0. Size of time index: ${activeSegment.timeIndex.entries}."
                    )
                    val newSegment = createAndDeleteSegment(newOffset, activeSegment, asyncDelete = true, LogRoll(this))
                    updateLogEndOffset(nextOffsetMetadata.messageOffset)
                    info(s"Rolled new log segment at offset $newOffset in ${time.hiResClockMs() - start} ms.")
                    return newSegment
                } else {
                    throw new KafkaException(s"Trying to roll a new log segment for topic partition $topicPartition with start offset $newOffset" +
                        s" =max(provided offset = $expectedNextOffset, LEO = $logEndOffset) while it already exists. Existing " +
                        s"segment is ${segments.get(newOffset)}.")
                }
            } else if (segments.nonEmpty && newOffset < activeSegment.baseOffset) {
                throw new KafkaException(
                    s"Trying to roll a new log segment for topic partition $topicPartition with " +
                        s"start offset $newOffset =max(provided offset = $expectedNextOffset, LEO = $logEndOffset) lower than start offset of the active segment $activeSegment")
            } else {
                val offsetIdxFile = LogFileUtils.offsetIndexFile(dir, newOffset)
                val timeIdxFile = LogFileUtils.timeIndexFile(dir, newOffset)
                val txnIdxFile = LogFileUtils.transactionIndexFile(dir, newOffset)

                for (file <- List(logFile, offsetIdxFile, timeIdxFile, txnIdxFile) if file.exists) {
                    warn(s"Newly rolled segment file ${file.getAbsolutePath} already exists; deleting it first")
                    Files.delete(file.toPath)
                }

                segments.lastSegment.ifPresent(_.onBecomeInactiveSegment())
            }

            // AutoMQ inject start
            val (newSegment, newSegmentCf) = createAndSaveSegment(logSegmentManager, "", logIdent)(newOffset, _dir, config, streamSliceManager, time)
            // Segment metadata and subsequent data appends are persisted through the same ordered WAL. Do not block
            // the append path on the metadata acknowledgement; a later append cannot be confirmed ahead of it.
            newSegmentCf.whenComplete((_, ex) => {
                if (ex != null) {
                    recordLogWriteFailedIfUnexpected(FutureUtil.cause(ex))
                }
            })
            // AutoMQ inject end
            segments.add(newSegment)

            // We need to update the segment base offset and append position data of the metadata when log rolls.
            // The next offset should not change.
            updateLogEndOffset(nextOffsetMetadata.messageOffset)

            info(s"Rolled new log segment at offset $newOffset in ${time.hiResClockMs() - start} ms.")

            newSegment
        }
    }

    override private[log] def truncateFullyAndStartAt(newOffset: Long): Iterable[LogSegment] = {
        val rst = super.truncateFullyAndStartAt(newOffset)
        _confirmOffset.set(logEndOffsetMetadata)
        rst
    }

    def snapshot(snapshot: PartitionSnapshot.Builder): Unit = {
        snapshot.logMeta(logSegmentManager.logMeta())
        snapshot.logEndOffset(logEndOffsetMetadata)
        logSegmentManager.streams().forEach(stream => {
            snapshot.streamEndOffset(stream.streamId(), stream.nextOffset())
            snapshot.addStreamLastAppendFuture(stream.lastAppendFuture());
        })
        val lastSegmentOpt = segments.lastSegment()
        if (lastSegmentOpt.isPresent) {
            snapshot.lastTimestampOffset(lastSegmentOpt.get().asInstanceOf[ElasticLogSegment].timeIndex().lastEntry())
        }
    }

    def snapshot(snapshot: PartitionSnapshot): Unit = {
        val logMeta = snapshot.logMeta()
        if (logMeta != null && !logMeta.getSegmentMetas.isEmpty) {
            logMeta.getStreamMap.forEach((name, streamId) => {
                streamManager.openIfNotExist(name, streamId)
            })
            segments.clear()
            logMeta.getSegmentMetas.forEach(segMeta => {
                val segment = new ElasticLogSegment(dir, segMeta, streamSliceManager, config, time, (_, _, _) => {}, logIdent)
                segments.add(segment)
            })
        }
        var logEndOffset = snapshot.logEndOffset()
        val segmentBaseOffset = segments.floorSegment(logEndOffset.messageOffset).get().baseOffset()
        logEndOffset = new LogOffsetMetadata(logEndOffset.messageOffset, segmentBaseOffset, logEndOffset.relativePositionInSegment);

        streamManager.streams().forEach((_, stream) => {
            val endOffset = snapshot.streamEndOffsets().get(stream.streamId())
            if (endOffset != null) {
                stream.confirmOffset(endOffset)
            }
        })
        val lastSegment = segments.lastSegment()
        if (lastSegment.isPresent) {
            lastSegment.get().asInstanceOf[ElasticLogSegment].snapshot(snapshot)
        }
        nextOffsetMetadata = logEndOffset
        _confirmOffset.set(logEndOffset)
    }
}

object ElasticLog extends Logging {
    override protected lazy val logger: Logger =
        Logger(AsyncLogger.wrap(LoggerFactory.getLogger(loggerName)))

    private val APPEND_PERMIT = Systems.getEnvInt("AUTOMQ_APPEND_PERMIT_SIZE",
        // autoscale the append permit size based on heap size, min 100MiB, max 1GiB, every 6GB heap add 100MiB permit
        Math.min(1024, 100 * Math.max(1, (Systems.HEAP_MEMORY_SIZE / (1024 * 1024 * 1024) / 6)).asInstanceOf[Int]) * 1024 * 1024
    )
    private val APPEND_PERMIT_SEMAPHORE = new Semaphore(APPEND_PERMIT)
    private val LOG_APPEND_PERMIT_NUM = Metrics.instance()
        .longGauge("kafka_stream_log_append_permit_num", "The number of permits in elastic log append limiter", "")
    LOG_APPEND_PERMIT_NUM.register(MetricsLevel.INFO, Attributes.empty(),
        measurement => measurement.record(APPEND_PERMIT_SEMAPHORE.availablePermits()))

    private val LAST_RECORD_TIMESTAMP = new AtomicLong()
    private val KafkaMetricsGroup = new KafkaMetricsGroup(ElasticLog.getClass)
    private val APPEND_PERMIT_ACQUIRE_FAIL_TIME_HIST = KafkaMetricsGroup.newHistogram("AppendPermitAcquireFailTimeNanos")
    private val APPEND_TIME_HIST = KafkaMetricsGroup.newHistogram("AppendTimeNanos")
    private val APPEND_CALLBACK_TIME_HIST = KafkaMetricsGroup.newHistogram("AppendCallbackTimeNanos")
    private val APPEND_ACK_TIME_HIST = KafkaMetricsGroup.newHistogram("AppendAckTimeNanos")
    private val APPEND_CALLBACK_EXECUTOR: Array[ExecutorService] = new Array[ExecutorService](Systems.CPU_CORES * 2)
    private val READ_ASYNC_EXECUTOR: Array[ExecutorService] = new Array[ExecutorService](Systems.CPU_CORES * 4)

    for (i <- APPEND_CALLBACK_EXECUTOR.indices) {
        APPEND_CALLBACK_EXECUTOR(i) = Executors.newSingleThreadExecutor(ThreadUtils.createThreadFactory("log-append-callback-executor-" + i, true))
    }
    for (i <- READ_ASYNC_EXECUTOR.indices) {
        READ_ASYNC_EXECUTOR(i) = Executors.newSingleThreadExecutor(ThreadUtils.createThreadFactory("log-read-async-executor-" + i, true))
    }

    private val META_SCHEDULE_EXECUTOR = Executors.newScheduledThreadPool(1, ThreadUtils.createThreadFactory("log-meta-schedule-executor", true))
    private val OPEN_CONTINUATION_EVENT_LOOPS = Array.tabulate(math.max(Systems.CPU_CORES / 2, 1))(index =>
        new EventLoop(s"elastic-log-open-continuation-$index"))
    private val LOG_LOADER_EXECUTOR = Threads.newCachedThreadPool(
        512, "elastic-log-loader-%d", true, LoggerFactory.getLogger(classOf[ElasticLog]))

    def formatStreamKey(namespace: String, topicPartition: TopicPartition, topicId: Option[Uuid]): String = {
        if (topicId.isEmpty) {
            namespace + "/" + topicPartition.topic() + "/" + topicPartition.partition()
        } else {
            namespace + "/" + topicId.get.toString + "/" + topicPartition.partition()
        }
    }

    def apply(client: Client, namespace: String, dir: File,
        config: LogConfig,
        scheduler: Scheduler,
        time: Time,
        topicPartition: TopicPartition,
        logDirFailureChannel: LogDirFailureChannel,
        numRemainingSegments: ConcurrentMap[String, Int] = new ConcurrentHashMap[String, Int],
        maxTransactionTimeoutMs: Int,
        producerStateManagerConfig: ProducerStateManagerConfig,
        topicId: Option[Uuid],
        leaderEpoch: Long,
        openStreamChecker: OpenStreamChecker,
        fastReassignmentManager: FastPartitionReassignmentManager = FastPartitionReassignmentManager.instance(),
        snapshotRead: Boolean = false,
        forceCleanShutdownRecovery: Boolean = false
    ): ElasticLog = {
        applyAsync(client, namespace, dir, config, scheduler, time, topicPartition, logDirFailureChannel,
            numRemainingSegments, maxTransactionTimeoutMs, producerStateManagerConfig, topicId, leaderEpoch,
            openStreamChecker, fastReassignmentManager, snapshotRead, forceCleanShutdownRecovery).join()
    }

    private case class RecoveredMetadata(metaStream: MetaStream,
        partitionMeta: ElasticPartitionMeta,
        producerStateManager: ElasticProducerStateManager,
        logMeta: ElasticLogMeta,
        leaderEpochCheckpointMeta: ElasticLeaderEpochCheckpointMeta)

    private case class OpenedStreams(metadata: RecoveredMetadata,
        streamManager: ElasticLogStreamManager)

    /**
     * Opens an ElasticLog without blocking on KV, stream lifecycle, MetaStream replay, or compensating cleanup.
     */
    def applyAsync(client: Client, namespace: String, dir: File,
        config: LogConfig,
        scheduler: Scheduler,
        time: Time,
        topicPartition: TopicPartition,
        logDirFailureChannel: LogDirFailureChannel,
        numRemainingSegments: ConcurrentMap[String, Int],
        maxTransactionTimeoutMs: Int,
        producerStateManagerConfig: ProducerStateManagerConfig,
        topicId: Option[Uuid],
        leaderEpoch: Long,
        openStreamChecker: OpenStreamChecker,
        fastReassignmentManager: FastPartitionReassignmentManager,
        snapshotRead: Boolean,
        forceCleanShutdownRecovery: Boolean): CompletableFuture[ElasticLog] = {
        val continuationExecutor = openContinuationExecutor(topicPartition)
        CompletableFuture.completedFuture[Void](null).thenComposeAsync(_ =>
            applyAsync0(client, namespace, dir, config, scheduler, time, topicPartition, logDirFailureChannel,
                numRemainingSegments, maxTransactionTimeoutMs, producerStateManagerConfig, topicId, leaderEpoch,
                openStreamChecker, fastReassignmentManager, snapshotRead, forceCleanShutdownRecovery,
                continuationExecutor), continuationExecutor)
    }

    private def applyAsync0(client: Client, namespace: String, dir: File,
        config: LogConfig,
        scheduler: Scheduler,
        time: Time,
        topicPartition: TopicPartition,
        logDirFailureChannel: LogDirFailureChannel,
        numRemainingSegments: ConcurrentMap[String, Int],
        maxTransactionTimeoutMs: Int,
        producerStateManagerConfig: ProducerStateManagerConfig,
        topicId: Option[Uuid],
        leaderEpoch: Long,
        openStreamChecker: OpenStreamChecker,
        fastReassignmentManager: FastPartitionReassignmentManager,
        snapshotRead: Boolean,
        forceCleanShutdownRecovery: Boolean,
        continuationExecutor: Executor): CompletableFuture[ElasticLog] = {
        logDirFailureChannel.clearOfflineLogDirRecord(dir.getPath)
        val logIdent = s"[ElasticLog partition=$topicPartition epoch=$leaderEpoch] "
        val topicIdStr: String = topicId.map(u => u.toString).getOrElse(topicPartition.topic())
        val replicationFactor = 1
        val streamTags = new util.HashMap[String, String]()
        streamTags.put(StreamTags.Topic.KEY, topicIdStr)
        streamTags.put(StreamTags.Partition.KEY, StreamTags.Partition.encode(topicPartition.partition()))

        if (snapshotRead) {
            return createSnapshotReadLog(client, dir, config, scheduler, time, topicPartition, logDirFailureChannel,
                maxTransactionTimeoutMs, producerStateManagerConfig, replicationFactor, leaderEpoch, streamTags,
                continuationExecutor)
        }

        val metaStreamRef = new AtomicReference[MetaStream]()
        val streamManagerRef = new AtomicReference[ElasticLogStreamManager]()
        val key = formatStreamKey(namespace, topicPartition, topicId)
        val metaStreamTopicId = topicId.getOrElse(Uuid.ZERO_UUID)
        val metaStreamFastReassignmentManager = if (topicId.isDefined) {
            fastReassignmentManager
        } else {
            FastPartitionReassignmentManager.disabled()
        }

        val metaStreamFuture = openMetaStream(client, key, replicationFactor, leaderEpoch, streamTags,
            metaStreamTopicId, topicPartition.partition(), metaStreamFastReassignmentManager, logIdent,
            openStreamChecker, continuationExecutor).thenApplyAsync(metaStream => {
            metaStreamRef.set(metaStream)
            metaStream
        }, continuationExecutor)

        val metadataFuture = metaStreamFuture.thenComposeAsync(metaStream =>
            recoverMetadata(metaStream, dir, topicPartition, maxTransactionTimeoutMs, producerStateManagerConfig,
                time, logIdent, continuationExecutor), continuationExecutor)

        val openedStreamsFuture = metadataFuture.thenComposeAsync(metadata => {
            openDataStreams(metadata, client, replicationFactor, leaderEpoch, streamTags).thenApplyAsync(opened => {
                val streamManager = opened.streamManager
                streamManagerRef.set(streamManager)
                opened
            }, continuationExecutor)
        }, continuationExecutor)

        val openFuture = openedStreamsFuture.thenComposeAsync(opened => CompletableFuture.supplyAsync(() =>
            loadElasticLog(opened, dir, config, scheduler, time, topicPartition, logDirFailureChannel,
                numRemainingSegments, leaderEpoch, forceCleanShutdownRecovery, logIdent), LOG_LOADER_EXECUTOR),
            continuationExecutor)

        openFuture.handle[CompletableFuture[ElasticLog]]((elasticLog, exception) => {
            if (exception == null) {
                CompletableFuture.completedFuture(elasticLog)
            } else {
                val cause = FutureUtil.cause(exception)
                if (cause.isInstanceOf[StreamFencedException]) {
                    warn(s"${logIdent}failed to open elastic log, trying to close streams.", cause)
                } else {
                    error(s"${logIdent}failed to open elastic log, trying to close streams.", cause)
                }
                closeOpenResources(metaStreamRef.get(), streamManagerRef.get()).handle[ElasticLog]((_, cleanupException) => {
                    if (cleanupException != null) {
                        val cleanupCause = FutureUtil.cause(cleanupException)
                        if (cleanupCause ne cause) {
                            cause.addSuppressed(cleanupCause)
                        }
                    }
                    throw new CompletionException(cause)
                })
            }
        }).thenCompose(future => future)
    }

    private def createSnapshotReadLog(client: Client,
        dir: File,
        config: LogConfig,
        scheduler: Scheduler,
        time: Time,
        topicPartition: TopicPartition,
        logDirFailureChannel: LogDirFailureChannel,
        maxTransactionTimeoutMs: Int,
        producerStateManagerConfig: ProducerStateManagerConfig,
        replicaCount: Int,
        leaderEpoch: Long,
        streamTags: util.Map[String, String],
        continuationExecutor: Executor): CompletableFuture[ElasticLog] = {
        ElasticLogStreamManager.create(Collections.emptyMap(), client.streamClient(), replicaCount, leaderEpoch,
            streamTags, true).thenApplyAsync(logStreamManager => {
                val streamSliceManager = new ElasticStreamSliceManager(logStreamManager)
                val segments = new CachedLogSegments(topicPartition)
                val partitionMeta = new ElasticPartitionMeta()
                val leaderEpochCheckpointMeta = new ElasticLeaderEpochCheckpointMeta(
                    LeaderEpochCheckpointFile.CURRENT_VERSION, new util.ArrayList[EpochEntry]())
                val producerStateManager = new ElasticProducerStateManager(topicPartition, dir,
                    maxTransactionTimeoutMs, producerStateManagerConfig, time,
                    new util.TreeMap[java.lang.Long, ByteBuffer](), _ => CompletableFuture.completedFuture(null))
                new ElasticLog(null, logStreamManager, streamSliceManager, producerStateManager, null, partitionMeta,
                    leaderEpochCheckpointMeta, dir, config, segments, new LogOffsetMetadata(0), scheduler, time,
                    topicPartition, logDirFailureChannel, 0, leaderEpoch, true)
            }, continuationExecutor)
    }

    private def openMetaStream(client: Client,
        key: String,
        replicaCount: Int,
        leaderEpoch: Long,
        streamTags: util.Map[String, String],
        topicId: Uuid,
        partitionId: Int,
        fastReassignmentManager: FastPartitionReassignmentManager,
        logIdent: String,
        openStreamChecker: OpenStreamChecker,
        continuationExecutor: Executor): CompletableFuture[MetaStream] = {
        client.kvClient().getKV(KeyValue.Key.of(key)).thenComposeAsync(value => {
            if (value.isNull) {
                createMetaStreamAsync(client, key, replicaCount, leaderEpoch, streamTags, topicId, partitionId,
                    fastReassignmentManager, logIdent, continuationExecutor)
            } else {
                val metaStreamId = Unpooled.wrappedBuffer(value.get()).readLong()
                awaitStreamReadyForOpen(openStreamChecker, topicId, partitionId, metaStreamId, leaderEpoch)
                    .thenComposeAsync(_ => client.streamClient().openStream(metaStreamId,
                        OpenStreamOptions.builder().epoch(leaderEpoch).tags(streamTags).build()), continuationExecutor)
                    .thenApply(stream => new MetaStream(stream, META_SCHEDULE_EXECUTOR, logIdent, topicId, partitionId,
                        fastReassignmentManager))
                    .thenApply(metaStream => {
                        info(s"${logIdent}opened existing meta stream: streamId=$metaStreamId")
                        metaStream
                    })
            }
        }, continuationExecutor)
    }

    private def recoverMetadata(metaStream: MetaStream,
        dir: File,
        topicPartition: TopicPartition,
        maxTransactionTimeoutMs: Int,
        producerStateManagerConfig: ProducerStateManagerConfig,
        time: Time,
        logIdent: String,
        continuationExecutor: Executor): CompletableFuture[RecoveredMetadata] = {
        metaStream.replayAsync(continuationExecutor).thenComposeAsync(metaMapJava => {
            val metaMap = metaMapJava.asScala
            val pendingMetadata = ListBuffer.empty[CompletableFuture[?]]
            val partitionMeta = metaMap.get(MetaStream.PARTITION_META_KEY)
                .map(_.asInstanceOf[ElasticPartitionMeta]).getOrElse {
                    val newMeta = new ElasticPartitionMeta(0, 0, 0)
                    pendingMetadata += metaStream.append(MetaKeyValue.of(MetaStream.PARTITION_META_KEY,
                        ElasticPartitionMeta.encode(newMeta)))
                    newMeta
                }
            info(s"${logIdent}loaded partition meta: $partitionMeta")

            val producerSnapshotsMeta = metaMap.get(MetaStream.PRODUCER_SNAPSHOTS_META_KEY)
                .map(_.asInstanceOf[ElasticPartitionProducerSnapshotsMeta])
                .getOrElse(new ElasticPartitionProducerSnapshotsMeta())
            val snapshotsMap = new ConcurrentSkipListMap[java.lang.Long, ByteBuffer](
                producerSnapshotsMeta.getSnapshots)
            if (!snapshotsMap.isEmpty) {
                info(s"${logIdent}loaded ${snapshotsMap.size} producer snapshots, " +
                    s"offsets(filenames) are ${snapshotsMap.keySet()} ")
            } else {
                info(s"${logIdent}loaded no producer snapshots")
            }
            val producerStateManager = new ElasticProducerStateManager(topicPartition, dir,
                maxTransactionTimeoutMs, producerStateManagerConfig, time, snapshotsMap,
                kv => metaStream.append(kv).thenApply(_ => null))
            val logMeta = metaMap.get(MetaStream.LOG_META_KEY)
                .map(_.asInstanceOf[ElasticLogMeta]).getOrElse(new ElasticLogMeta())
            val leaderEpochCheckpointMeta = metaMap.get(MetaStream.LEADER_EPOCH_CHECKPOINT_KEY)
                .map(_.asInstanceOf[ElasticLeaderEpochCheckpointMeta]).getOrElse {
                    val newMeta = new ElasticLeaderEpochCheckpointMeta(LeaderEpochCheckpointFile.CURRENT_VERSION,
                        List.empty[EpochEntry].asJava)
                    pendingMetadata += metaStream.append(MetaKeyValue.of(MetaStream.LEADER_EPOCH_CHECKPOINT_KEY,
                        ByteBuffer.wrap(newMeta.encode())))
                    newMeta
                }
            val metadata = RecoveredMetadata(metaStream, partitionMeta, producerStateManager, logMeta,
                leaderEpochCheckpointMeta)
            CompletableFuture.allOf(pendingMetadata.toArray: _*).thenApply(_ => metadata)
        }, continuationExecutor)
    }

    private def openDataStreams(metadata: RecoveredMetadata,
        client: Client,
        replicaCount: Int,
        leaderEpoch: Long,
        streamTags: util.Map[String, String]): CompletableFuture[OpenedStreams] = {
        ElasticLogStreamManager.create(metadata.logMeta.getStreamMap, client.streamClient(), replicaCount,
            leaderEpoch, streamTags, false).thenApply(streamManager => OpenedStreams(metadata, streamManager))
    }

    private def loadElasticLog(opened: OpenedStreams,
        dir: File,
        config: LogConfig,
        scheduler: Scheduler,
        time: Time,
        topicPartition: TopicPartition,
        logDirFailureChannel: LogDirFailureChannel,
        numRemainingSegments: ConcurrentMap[String, Int],
        leaderEpoch: Long,
        forceCleanShutdownRecovery: Boolean,
        logIdent: String): ElasticLog = {
        val metadata = opened.metadata
        val logStreamManager = opened.streamManager
        val streamSliceManager = new ElasticStreamSliceManager(logStreamManager)
        val logSegmentManager = new ElasticLogSegmentManager(metadata.metaStream, logStreamManager, logIdent)
        val segments = new CachedLogSegments(topicPartition)
        val offsets = new ElasticLogLoader(
            metadata.logMeta,
            segments,
            logSegmentManager,
            streamSliceManager,
            dir,
            topicPartition,
            config,
            time,
            hadCleanShutdown = metadata.partitionMeta.getCleanedShutdown || forceCleanShutdownRecovery,
            logStartOffsetCheckpoint = metadata.partitionMeta.getStartOffset,
            metadata.partitionMeta.getRecoverOffset,
            Optional.empty(),
            producerStateManager = metadata.producerStateManager,
            numRemainingSegments = numRemainingSegments,
            createAndSaveSegmentFunc = createAndSaveSegment(logSegmentManager, logIdent = logIdent)).load()
        info(s"${logIdent}loaded log meta: ${metadata.logMeta}")
        info(s"${logIdent}loaded leader epoch checkpoint with " +
            s"${metadata.leaderEpochCheckpointMeta.entries.size} entries")
        if (!metadata.leaderEpochCheckpointMeta.entries.isEmpty) {
            val lastEntry = metadata.leaderEpochCheckpointMeta.entries.get(
                metadata.leaderEpochCheckpointMeta.entries.size - 1)
            info(s"${logIdent}last leaderEpoch entry is: $lastEntry")
        }
        val elasticLog = new ElasticLog(metadata.metaStream, logStreamManager, streamSliceManager,
            metadata.producerStateManager, logSegmentManager, metadata.partitionMeta,
            metadata.leaderEpochCheckpointMeta, dir, config, segments, offsets.nextOffsetMetadata, scheduler, time,
            topicPartition, logDirFailureChannel, offsets.logStartOffset, leaderEpoch)
        if (metadata.partitionMeta.getCleanedShutdown) {
            metadata.partitionMeta.setCleanedShutdown(false)
            elasticLog.asyncPersistPartitionMeta()
        }
        elasticLog
    }

    private def closeOpenResources(metaStream: MetaStream,
        streamManager: ElasticLogStreamManager): CompletableFuture[Void] = {
        val closeFutures = ListBuffer.empty[CompletableFuture[?]]
        if (metaStream != null) {
            closeFutures += metaStream.close()
        }
        if (streamManager != null) {
            closeFutures += streamManager.close()
        }
        CompletableFuture.allOf(closeFutures.toArray: _*)
    }

    /**
     * Destroy related streams of the targeted partition.
     *
     * @param client         elastic stream client
     * @param namespace      namespace
     * @param topicPartition topic partition
     * @param currentEpoch   current epoch of the partition
     * @return Unit
     */
    def destroy(client: Client, namespace: String, topicPartition: TopicPartition, topicId: Uuid,
        currentEpoch: Long): Unit = {
        val logIdent = s"[ElasticLog partition=$topicPartition topicId=$topicId] "

        val key = formatStreamKey(namespace, topicPartition, Some(topicId))
        var metaStreamIdOpt: Option[Long] = None

        try {
            val value = client.kvClient().getKV(KeyValue.Key.of(key)).get()
            val metaStreamId = Unpooled.wrappedBuffer(value.get()).readLong()
            metaStreamIdOpt = Some(metaStreamId)
        } finally {
            // remove kv info
            client.kvClient().delKV(KeyValue.Key.of(key)).get()
        }

        if (metaStreamIdOpt.isEmpty) {
            warn(s"$logIdent meta stream not exists for topicPartition $topicPartition, skip destroy")
            return
        }

        // First, open partition meta stream with higher epoch.
        val metaStream = new MetaStream(
            openStreamWithRetry(client, metaStreamIdOpt.get, currentEpoch + 1),
            META_SCHEDULE_EXECUTOR,
            logIdent,
            topicId,
            topicPartition.partition(),
            FastPartitionReassignmentManager.disabled())
        info(s"$logIdent opened meta stream: streamId=${metaStreamIdOpt.get}, epoch=${currentEpoch + 1}")
        // fetch metas(log meta, producer snapshot, partition meta, ...) from meta stream
        val metaMap = metaStream.replay().asScala

        metaMap.get(MetaStream.LOG_META_KEY).map(m => m.asInstanceOf[ElasticLogMeta]).foreach(logMeta => {
            // Then, destroy log stream, time index stream, txn stream, ...
            // streamId <0 means the stream is not actually created.
            logMeta.getStreamMap.values().forEach(streamId => if (streamId >= 0) {
                openStreamWithRetry(client, streamId, currentEpoch + 1).destroy()
                info(s"$logIdent destroyed stream: streamId=$streamId, epoch=${currentEpoch + 1}")
            })
        })

        // Finally, destroy meta stream.
        metaStream.destroy()
        info(s"$logIdent Destroyed with epoch ${currentEpoch + 1}")
    }

    private def openStreamWithRetry(client: Client, streamId: Long, epoch: Long): Stream = {
        client.streamClient()
            .openStream(streamId, OpenStreamOptions.builder().epoch(epoch).build())
            .exceptionally(_ => client.streamClient()
                .openStream(streamId, OpenStreamOptions.builder().build()).join()
            ).join()
    }

    private def createMetaStreamAsync(client: Client, key: String, replicaCount: Int, leaderEpoch: Long,
        streamTags: util.Map[String, String], topicId: Uuid, partitionId: Int,
        fastReassignmentManager: FastPartitionReassignmentManager,
        logIdent: String,
        continuationExecutor: Executor): CompletableFuture[MetaStream] = {
        val options = CreateStreamOptions.builder().replicaCount(replicaCount).epoch(leaderEpoch)
        streamTags.forEach((k, v) => options.tag(k, v))
        client.streamClient().createAndOpenStream(options.build())
            .thenApply(stream => new MetaStream(stream, META_SCHEDULE_EXECUTOR, logIdent, topicId, partitionId,
                fastReassignmentManager))
            .thenComposeAsync(metaStream => {
                val streamId = metaStream.streamId()
                info(s"${logIdent}created meta stream for $key, streamId: $streamId")
                val valueBuf = ByteBuffer.allocate(8)
                valueBuf.putLong(streamId)
                valueBuf.flip()
                client.kvClient().putKVIfAbsent(KeyValue.of(key, valueBuf)).thenApply(_ => metaStream)
                    .handle[CompletableFuture[MetaStream]]((result, exception) => {
                        if (exception == null) {
                            CompletableFuture.completedFuture(result)
                        } else {
                            val cause = FutureUtil.cause(exception)
                            metaStream.close().handle[MetaStream]((_, cleanupException) => {
                                if (cleanupException != null) {
                                    val cleanupCause = FutureUtil.cause(cleanupException)
                                    if (cleanupCause ne cause) {
                                        cause.addSuppressed(cleanupCause)
                                    }
                                }
                                throw new CompletionException(cause)
                            })
                        }
                    }).thenCompose(future => future)
            }, continuationExecutor)
    }

    private def openContinuationExecutor(topicPartition: TopicPartition): EventLoop =
        OPEN_CONTINUATION_EVENT_LOOPS(Math.floorMod(topicPartition.hashCode(), OPEN_CONTINUATION_EVENT_LOOPS.length))

    private def persistMeta(metaStream: MetaStream, metaKeyValue: MetaKeyValue): Unit = {
        metaStream.appendSync(metaKeyValue)
    }

    /**
     * Create a new segment and save the meta in metaStream if needed. This method can be used to create a new normal segment or a new cleaned segment.
     * For the newly created segment, the meta will immediately be saved in metaStream.
     * For the newly created cleaned segment, the meta should not be saved here. It will be saved if the replacement happens.
     */
    private def createAndSaveSegment(logSegmentManager: ElasticLogSegmentManager, suffix: String = "", logIdent: String)
        (baseOffset: Long, dir: File, config: LogConfig, streamSliceManager: ElasticStreamSliceManager, time: Time)
    : (ElasticLogSegment, CompletableFuture[Void]) = {
        if (!suffix.equals("") && !suffix.equals(LocalLog.CleanedFileSuffix)) {
            throw new IllegalArgumentException("suffix must be empty or " + LocalLog.CleanedFileSuffix)
        }
        val meta = new ElasticStreamSegmentMeta()
        meta.baseOffset(baseOffset)
        meta.streamSuffix(suffix)
        meta.createTimestamp(time.milliseconds())
        val segment: ElasticLogSegment = new ElasticLogSegment(dir, meta, streamSliceManager, config, time, logSegmentManager.logSegmentEventListener(), logIdent)
        var metaSaveCf: CompletableFuture[Void] = CompletableFuture.completedFuture(null)
        if (suffix.equals("")) {
            metaSaveCf = logSegmentManager.create(baseOffset, segment)
        } else if (suffix.equals(UnifiedLog.CleanedFileSuffix)) {
            logSegmentManager.putInflightCleaned(baseOffset, segment)
        }

        info(s"${logIdent}Created a new log segment with baseOffset = $baseOffset, suffix = $suffix")
        (segment, metaSaveCf)
    }

    private[log] def maybeHandleIOExceptionAsync[T](logDirFailureChannel: LogDirFailureChannel,
        logDir: String,
        errorMsg: => String)(fun: => CompletableFuture[T]): CompletableFuture[T] = {
        if (logDirFailureChannel.hasOfflineLogDir(logDir)) {
            return CompletableFuture.failedFuture(new KafkaStorageException(s"The log dir $logDir is already offline due to a previous IO exception."))
        }
        val resultCf = new CompletableFuture[T]()
        fun.whenComplete((result, exception) => {
            if (exception != null) {
                exception match {
                    case exception1: IOException =>
                        logDirFailureChannel.maybeAddOfflineLogDir(logDir, errorMsg, exception1)
                        resultCf.completeExceptionally(new KafkaStorageException(errorMsg, exception1))
                    case _ => resultCf.completeExceptionally(exception)
                }
            } else {
                resultCf.complete(result)
            }
        })
        resultCf
    }

    private def awaitStreamReadyForOpen(checker: OpenStreamChecker, topicId: Uuid, partition: Int,
        streamId: Long, epoch: Long): CompletableFuture[Void] = {
        checker.check(topicId, partition, streamId, epoch)
    }
}
