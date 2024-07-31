/*
 * Copyright 2024, AutoMQ HK Limited.
 *
 * The use of this file is governed by the Business Source License,
 * as detailed in the file "/LICENSE.S3Stream" included in this repository.
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */

package kafka.log.streamaspect

import kafka.utils.Logging
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.errors.InvalidOffsetException
import org.apache.kafka.common.utils.Time
import org.apache.kafka.storage.internals.epoch.LeaderEpochFileCache
import org.apache.kafka.storage.internals.log._

import java.io.File
import java.util.Optional
import java.util.concurrent.{CompletableFuture, ConcurrentHashMap, ConcurrentMap}
import scala.jdk.CollectionConverters.IteratorHasAsScala

/**
 * ref. LogLoader
 */
class ElasticLogLoader(logMeta: ElasticLogMeta,
    segments: LogSegments,
    logSegmentsManager: ElasticLogSegmentManager,
    streamSliceManager: ElasticStreamSliceManager,
    dir: File,
    topicPartition: TopicPartition,
    config: LogConfig,
    time: Time,
    hadCleanShutdown: Boolean,
    logStartOffsetCheckpoint: Long,
    recoveryPointCheckpoint: Long,
    leaderEpochCache: Optional[LeaderEpochFileCache],
    producerStateManager: ProducerStateManager,
    numRemainingSegments: ConcurrentMap[String, Int] = new ConcurrentHashMap[String, Int],
    createAndSaveSegmentFunc: (Long, File, LogConfig, ElasticStreamSliceManager, Time) => (ElasticLogSegment, CompletableFuture[Void]))
    extends Logging {
    logIdent = s"[ElasticLogLoader partition=$topicPartition] "

    /**
     * Load the log segments from the log files on disk, and returns the components of the loaded log.
     * Additionally, it also suitably updates the provided LeaderEpochFileCache and ProducerStateManager
     * to reflect the contents of the loaded log.
     *
     * In the context of the calling thread, this function does not need to convert IOException to
     * KafkaStorageException because it is only called before all logs are loaded.
     *
     * @return the offsets of the Log successfully loaded from disk
     * @throws LogSegmentOffsetOverflowException if we encounter a .swap file with messages that
     *                                           overflow index offset
     */
    def load(): LoadedLogOffsets = {
        // remove cleaned segment from log meta.
        removeTempFiles()

        // load all segments
        loadSegments()

        // make sure the producer state manager endOffset is less than or equal to the recoveryPointCheckpoint
        producerStateManager.truncateAndReload(logStartOffsetCheckpoint, recoveryPointCheckpoint, time.milliseconds())
        val (newRecoveryPoint: Long, nextOffset: Long) = {
            recoverLog()
        }

        val newLogStartOffset = math.max(logStartOffsetCheckpoint, segments.firstSegment.get.baseOffset)

        val activeSegment = segments.lastSegment.get
        new LoadedLogOffsets(
            newLogStartOffset,
            newRecoveryPoint,
            new LogOffsetMetadata(nextOffset, activeSegment.baseOffset, activeSegment.size))
    }

    // do nothing
    private def removeTempFiles(): Unit = {
    }

    private def loadSegments(): Unit = {
        logMeta.getSegmentMetas.forEach(segmentMeta => {
            val segment = new ElasticLogSegment(dir, segmentMeta, streamSliceManager, config, time, logSegmentsManager.logSegmentEventListener(), logIdent)
            segments.add(segment)
            logSegmentsManager.put(segment.baseOffset, segment)
        })
    }

    /**
     * Just recovers the given segment, without adding it to the provided params.segments.
     *
     * @param segment Segment to recover
     * @return The number of bytes truncated from the segment
     * @throws LogSegmentOffsetOverflowException if the segment contains messages that cause index offset overflow
     */
    private def recoverSegment(segment: LogSegment): Int = {
        val bytesTruncated = segment.recover(producerStateManager, leaderEpochCache)
        // once we have recovered the segment's data, take a snapshot to ensure that we won't
        // need to reload the same segment again while recovering another segment.
        producerStateManager.takeSnapshot()
        bytesTruncated
    }

    /**
     * Recover the log segments (if there was an unclean shutdown). Ensures there is at least one
     * active segment, and returns the updated recovery point and next offset after recovery. Along
     * the way, the method suitably updates ProducerStateManager inside
     * the provided LogComponents.
     *
     * This method does not need to convert IOException to KafkaStorageException because it is only
     * called before all logs are loaded.
     *
     * @return a tuple containing (newRecoveryPoint, nextOffset).
     * @throws LogSegmentOffsetOverflowException if we encountered a legacy segment with offset overflow
     */
    private[log] def recoverLog(): (Long, Long) = {
        /** return the log end offset if valid */
        def deleteSegmentsIfLogStartGreaterThanLogEnd(): Option[Long] = {
            if (segments.nonEmpty) {
                val logEndOffset = segments.lastSegment.get.readNextOffset
                if (logEndOffset >= logStartOffsetCheckpoint)
                    Some(logEndOffset)
                else {
                    // wont' happen
                    throw new IllegalStateException()
                }
            } else None
        }

        // If we have the clean shutdown marker, skip recovery.
        if (!hadCleanShutdown) {
            val unflushed = segments.values(recoveryPointCheckpoint, Long.MaxValue)
            val numUnflushed = unflushed.size
            val unflushedIter = unflushed.iterator
            var truncated = false
            var numFlushed = 0
            val threadName = Thread.currentThread().getName
            numRemainingSegments.put(threadName, numUnflushed)
            while (unflushedIter.hasNext && !truncated) {
                val segment = unflushedIter.next()
                info(s"Recovering unflushed segment ${segment.baseOffset}. $numFlushed/$numUnflushed recovered for $topicPartition.")

                val truncatedBytes =
                    try {
                        recoverSegment(segment)
                    } catch {
                        case e: InvalidOffsetException =>
                            val startOffset = segment.baseOffset
                            warn(s"Found invalid offset during recovery. Deleting the" +
                                s" corrupt segment and creating an empty one with starting offset $startOffset", e)
                            // We use appended offset here to decide whether to truncate the segment.
                            // It is equivalent to 'truncatedBytes' since we only care if the segment has any data.
                            segment.asInstanceOf[ElasticLogSegment].appendedOffset
                    }
                if (truncatedBytes > 0) {
                    // we had an invalid message, delete all remaining log
                    warn(s"Corruption found in segment ${segment.baseOffset}," +
                        s" truncating to offset ${segment.readNextOffset}")
                    // remove all other segments
                    removeAndDeleteSegmentsAsync(unflushedIter.asScala.toVector)
                    truncated = true
                    // segment is truncated, so set remaining segments to 0
                    numRemainingSegments.put(threadName, 0)
                } else {
                    numFlushed += 1
                    numRemainingSegments.put(threadName, numUnflushed - numFlushed)
                }
            }

            if (truncated) {
                val baseOffset = segments.lastSegment.get.baseOffset
                // remove the targeted segment and recreate an empty one with the correct base offset
                removeAndDeleteSegmentsAsync(List(segments.lastSegment.get))
                createAndAddToSegments(baseOffset)
            }
        }

        val logEndOffsetOption = deleteSegmentsIfLogStartGreaterThanLogEnd()

        if (segments.isEmpty) {
            // no existing segments, create a new mutable segment beginning at logStartOffset
            createAndAddToSegments(logStartOffsetCheckpoint)
            // No need to put it into segmentMap since it was done in 'createAndSaveSegmentFunc'.
        }

        // Update the recovery point if there was a clean shutdown and did not perform any changes to
        // the segment. Otherwise, we just ensure that the recovery point is not ahead of the log end
        // offset. To ensure correctness and to make it easier to reason about, it's best to only advance
        // the recovery point when the log is flushed. If we advanced the recovery point here, we could
        // skip recovery for unflushed segments if the broker crashed after we checkpoint the recovery
        // point and before we flush the segment.
        (hadCleanShutdown, logEndOffsetOption) match {
            case (true, Some(logEndOffset)) =>
                (logEndOffset, logEndOffset)
            case _ =>
                val logEndOffset = logEndOffsetOption.getOrElse(segments.lastSegment.get.readNextOffset)
                (Math.min(recoveryPointCheckpoint, logEndOffset), logEndOffset)
        }
    }

    private def createAndAddToSegments(baseOffset: Long): Unit = {
        val (segment, _) = createAndSaveSegmentFunc(baseOffset, dir, config, streamSliceManager, time)
        segments.add(segment)
    }

    /**
     * This method deletes the given log segments and the associated producer snapshots, by doing the
     * following for each of them:
     *  - It removes the segment from the segment map so that it will no longer be used for reads.
     *
     * This method does not need to convert IOException to KafkaStorageException because it is either
     * called before all logs are loaded or the immediate caller will catch and handle IOException
     *
     * @param segmentsToDelete The log segments to schedule for deletion
     */
    private def removeAndDeleteSegmentsAsync(segmentsToDelete: Iterable[LogSegment]): Unit = {
        if (segmentsToDelete.nonEmpty) {
            // Most callers hold an iterator into the `params.segments` collection and
            // `removeAndDeleteSegmentAsync` mutates it by removing the deleted segment. Therefore,
            // we should force materialization of the iterator here, so that results of the iteration
            // remain valid and deterministic. We should also pass only the materialized view of the
            // iterator to the logic that deletes the segments.
            val toDelete = segmentsToDelete.toList
            info(s"Deleting segments as part of log recovery: ${toDelete.mkString(",")}")
            toDelete.foreach { segment =>
                segments.remove(segment.baseOffset)
                logSegmentsManager.remove(segment.baseOffset)
            }
        }
    }
}
