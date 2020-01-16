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
package org.apache.kafka.streams.processor.internals;

import org.apache.kafka.clients.consumer.CommitFailedException;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.AuthorizationException;
import org.apache.kafka.common.errors.ProducerFencedException;
import org.apache.kafka.common.errors.RebalanceInProgressException;
import org.apache.kafka.common.errors.TimeoutException;
import org.apache.kafka.common.errors.UnknownProducerIdException;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.metrics.Sensor;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.errors.DeserializationExceptionHandler;
import org.apache.kafka.streams.errors.ProcessorStateException;
import org.apache.kafka.streams.errors.ProductionExceptionHandler;
import org.apache.kafka.streams.errors.StreamsException;
import org.apache.kafka.streams.errors.TaskMigratedException;
import org.apache.kafka.streams.processor.internals.metrics.StreamsMetricsImpl.Version;
import org.apache.kafka.streams.processor.internals.metrics.TaskMetrics;
import org.apache.kafka.streams.processor.Cancellable;
import org.apache.kafka.streams.processor.PunctuationType;
import org.apache.kafka.streams.processor.Punctuator;
import org.apache.kafka.streams.processor.TaskId;
import org.apache.kafka.streams.processor.TimestampExtractor;
import org.apache.kafka.streams.processor.internals.metrics.StreamsMetricsImpl;
import org.apache.kafka.streams.processor.internals.metrics.ThreadMetrics;
import org.apache.kafka.streams.state.internals.ThreadCache;

import java.io.IOException;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.nio.ByteBuffer;
import java.util.Base64;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import static java.lang.String.format;
import static java.util.Collections.singleton;
import static org.apache.kafka.streams.processor.internals.metrics.StreamsMetricsImpl.maybeMeasureLatency;

/**
 * A StreamTask is associated with a {@link PartitionGroup}, and is assigned to a StreamThread for processing.
 */
public class StreamTask extends AbstractTask implements ProcessorNodePunctuator {

    private static final ConsumerRecord<Object, Object> DUMMY_RECORD = new ConsumerRecord<>(ProcessorContextImpl.NONEXIST_TOPIC, -1, -1L, null, null);
    // visible for testing
    static final byte LATEST_MAGIC_BYTE = 1;

    private final Time time;
    private final long maxTaskIdleMs;
    private final int maxBufferedSize;
    private final StreamsMetricsImpl streamsMetrics;
    private final PartitionGroup partitionGroup;
    private final RecordCollector recordCollector;
    private final PartitionGroup.RecordInfo recordInfo;
    private final Map<TopicPartition, Long> consumedOffsets;
    private final PunctuationQueue streamTimePunctuationQueue;
    private final PunctuationQueue systemTimePunctuationQueue;
    private final ProducerSupplier producerSupplier;

    private final Sensor closeTaskSensor;
    private final Sensor processLatencySensor;
    private final Sensor punctuateLatencySensor;
    private final Sensor commitSensor;
    private final Sensor enforcedProcessingSensor;
    private final Sensor recordLatenessSensor;

    private long idleStartTime;
    private Producer<byte[], byte[]> producer;
    private boolean commitRequested = false;
    private boolean transactionInFlight = false;

    private final String threadId;

    public interface ProducerSupplier {
        Producer<byte[], byte[]> get();
    }

    public StreamTask(final TaskId id,
                      final Set<TopicPartition> partitions,
                      final ProcessorTopology topology,
                      final Consumer<byte[], byte[]> consumer,
                      final ChangelogReader changelogReader,
                      final StreamsConfig config,
                      final StreamsMetricsImpl metrics,
                      final StateDirectory stateDirectory,
                      final ThreadCache cache,
                      final Time time,
                      final ProducerSupplier producerSupplier) {
        this(id, partitions, topology, consumer, changelogReader, config, metrics, stateDirectory, cache, time, producerSupplier, null);
    }

    public StreamTask(final TaskId id,
                      final Set<TopicPartition> partitions,
                      final ProcessorTopology topology,
                      final Consumer<byte[], byte[]> consumer,
                      final ChangelogReader changelogReader,
                      final StreamsConfig config,
                      final StreamsMetricsImpl streamsMetrics,
                      final StateDirectory stateDirectory,
                      final ThreadCache cache,
                      final Time time,
                      final ProducerSupplier producerSupplier,
                      final RecordCollector recordCollector) {
        super(id, partitions, topology, consumer, changelogReader, false, stateDirectory, config);

        this.time = time;
        this.producerSupplier = producerSupplier;
        this.producer = producerSupplier.get();
        this.streamsMetrics = streamsMetrics;

        threadId = Thread.currentThread().getName();
        final String taskId = id.toString();
        closeTaskSensor = ThreadMetrics.closeTaskSensor(threadId, streamsMetrics);
        if (streamsMetrics.version() == Version.FROM_0100_TO_24) {
            final Sensor parent = ThreadMetrics.commitOverTasksSensor(threadId, streamsMetrics);
            commitSensor = TaskMetrics.commitSensor(threadId, taskId, streamsMetrics, parent);
            enforcedProcessingSensor = TaskMetrics.enforcedProcessingSensor(threadId, taskId, streamsMetrics, parent);
        } else {
            commitSensor = TaskMetrics.commitSensor(threadId, taskId, streamsMetrics);
            enforcedProcessingSensor = TaskMetrics.enforcedProcessingSensor(threadId, taskId, streamsMetrics);
        }
        processLatencySensor = TaskMetrics.processLatencySensor(threadId, taskId, streamsMetrics);
        punctuateLatencySensor = TaskMetrics.punctuateSensor(threadId, taskId, streamsMetrics);
        recordLatenessSensor = TaskMetrics.recordLatenessSensor(threadId, taskId, streamsMetrics);
        TaskMetrics.droppedRecordsSensor(threadId, taskId, streamsMetrics);

        final ProductionExceptionHandler productionExceptionHandler = config.defaultProductionExceptionHandler();

        if (recordCollector == null) {
            this.recordCollector = new RecordCollectorImpl(
                taskId,
                logContext,
                productionExceptionHandler,
                TaskMetrics.droppedRecordsSensorOrSkippedRecordsSensor(threadId, taskId, streamsMetrics)
            );
        } else {
            this.recordCollector = recordCollector;
        }
        this.recordCollector.init(this.producer);

        streamTimePunctuationQueue = new PunctuationQueue();
        systemTimePunctuationQueue = new PunctuationQueue();
        maxTaskIdleMs = config.getLong(StreamsConfig.MAX_TASK_IDLE_MS_CONFIG);
        maxBufferedSize = config.getInt(StreamsConfig.BUFFERED_RECORDS_PER_PARTITION_CONFIG);

        // initialize the consumed and committed offset cache
        consumedOffsets = new HashMap<>();

        // create queues for each assigned partition and associate them
        // to corresponding source nodes in the processor topology
        final Map<TopicPartition, RecordQueue> partitionQueues = new HashMap<>();

        // initialize the topology with its own context
        processorContext = new ProcessorContextImpl(id, this, config, this.recordCollector, stateMgr, streamsMetrics, cache);

        final TimestampExtractor defaultTimestampExtractor = config.defaultTimestampExtractor();
        final DeserializationExceptionHandler defaultDeserializationExceptionHandler = config.defaultDeserializationExceptionHandler();
        for (final TopicPartition partition : partitions) {
            final SourceNode source = topology.source(partition.topic());
            final TimestampExtractor sourceTimestampExtractor = source.getTimestampExtractor() != null ? source.getTimestampExtractor() : defaultTimestampExtractor;
            final RecordQueue queue = new RecordQueue(
                partition,
                source,
                sourceTimestampExtractor,
                defaultDeserializationExceptionHandler,
                processorContext,
                logContext
            );
            partitionQueues.put(partition, queue);
        }

        recordInfo = new PartitionGroup.RecordInfo();
        partitionGroup = new PartitionGroup(partitionQueues, recordLatenessSensor);

        stateMgr.registerGlobalStateStores(topology.globalStateStores());

        // initialize transactions if eos is turned on, which will block if the previous transaction has not
        // completed yet; do not start the first transaction until the topology has been initialized later
        if (eosEnabled) {
            initializeTransactions();
        }
    }

    @Override
    public void initializeMetadata() {
        try {
            final Map<TopicPartition, OffsetAndMetadata> offsetsAndMetadata = consumer.committed(partitions).entrySet().stream()
                .filter(e -> e.getValue() != null)
                .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
            initializeCommittedOffsets(offsetsAndMetadata);
            initializeTaskTime(offsetsAndMetadata);
        } catch (final AuthorizationException e) {
            throw new ProcessorStateException(String.format("task [%s] AuthorizationException when initializing offsets for %s", id, partitions), e);
        } catch (final WakeupException e) {
            throw e;
        } catch (final KafkaException e) {
            throw new ProcessorStateException(String.format("task [%s] Failed to initialize offsets for %s", id, partitions), e);
        }
    }

    private void initializeCommittedOffsets(final Map<TopicPartition, OffsetAndMetadata> offsetsAndMetadata) {
        // Currently there is no easy way to tell the ProcessorStateManager to only restore up to
        // a specific offset. In most cases this is fine. However, in optimized topologies we can
        // have a source topic that also serves as a changelog, and in this case we want our active
        // stream task to only play records up to the last consumer committed offset. Here we find
        // partitions of topics that are both sources and changelogs and set the consumer committed
        // offset via stateMgr as there is not a more direct route.
        final Set<String> changelogTopicNames = new HashSet<>(topology.storeToChangelogTopic().values());
        final Map<TopicPartition, Long> committedOffsetsForChangelogs = offsetsAndMetadata
            .entrySet()
            .stream()
            .filter(e -> changelogTopicNames.contains(e.getKey().topic()))
            .collect(Collectors.toMap(Map.Entry::getKey, e -> e.getValue().offset()));

        // those do not have a committed offset would default to 0
        for (final TopicPartition tp : partitions) {
            committedOffsetsForChangelogs.putIfAbsent(tp, 0L);
        }

        stateMgr.putOffsetLimits(committedOffsetsForChangelogs);
    }

    private void initializeTaskTime(final Map<TopicPartition, OffsetAndMetadata> offsetsAndMetadata) {
        for (final Map.Entry<TopicPartition, OffsetAndMetadata> entry : offsetsAndMetadata.entrySet()) {
            final TopicPartition partition = entry.getKey();
            final OffsetAndMetadata metadata = entry.getValue();

            if (metadata != null) {
                final long committedTimestamp = decodeTimestamp(metadata.metadata());
                partitionGroup.setPartitionTime(partition, committedTimestamp);
                log.debug("A committed timestamp was detected: setting the partition time of partition {}"
                    + " to {} in stream task {}", partition, committedTimestamp, id);
            } else {
                log.debug("No committed timestamp was found in metadata for partition {}", partition);
            }
        }

        final Set<TopicPartition> nonCommitted = new HashSet<>(partitions);
        nonCommitted.removeAll(offsetsAndMetadata.keySet());
        for (final TopicPartition partition : nonCommitted) {
            log.debug("No committed offset for partition {}, therefore no timestamp can be found for this partition", partition);
        }
    }


    @Override
    public boolean initializeStateStores() {
        log.debug("Initializing state stores");
        registerStateStores();
        return changelogPartitions().isEmpty();
    }

    /**
     * <pre>
     * - (re-)initialize the topology of the task
     * </pre>
     *
     * @throws TaskMigratedException if the task producer got fenced (EOS only)
     */
    @Override
    public void initializeTopology() {
        initTopology();

        if (eosEnabled) {
            try {
                this.producer.beginTransaction();
            } catch (final ProducerFencedException | UnknownProducerIdException e) {
                throw new TaskMigratedException(this, e);
            }
            transactionInFlight = true;
        }

        processorContext.initialize();

        taskInitialized = true;

        idleStartTime = RecordQueue.UNKNOWN;

        stateMgr.ensureStoresRegistered();
    }

    /**
     * <pre>
     * - resume the task
     * </pre>
     */
    @Override
    public void resume() {
        log.debug("Resuming");
        if (eosEnabled) {
            if (producer != null) {
                throw new IllegalStateException("Task producer should be null.");
            }
            producer = producerSupplier.get();
            initializeTransactions();
            recordCollector.init(producer);

            try {
                stateMgr.clearCheckpoints();
            } catch (final IOException e) {
                throw new ProcessorStateException(format("%sError while deleting the checkpoint file", logPrefix), e);
            }
        }
        initializeMetadata();
    }

    /**
     * An active task is processable if its buffer contains data for all of its input
     * source topic partitions, or if it is enforced to be processable
     */
    boolean isProcessable(final long now) {
        if (partitionGroup.allPartitionsBuffered()) {
            idleStartTime = RecordQueue.UNKNOWN;
            return true;
        } else if (partitionGroup.numBuffered() > 0) {
            if (idleStartTime == RecordQueue.UNKNOWN) {
                idleStartTime = now;
            }

            if (now - idleStartTime >= maxTaskIdleMs) {
                enforcedProcessingSensor.record();
                return true;
            } else {
                return false;
            }
        } else {
            // there's no data in any of the topics; we should reset the enforced
            // processing timer
            idleStartTime = RecordQueue.UNKNOWN;
            return false;
        }
    }

    /**
     * Process one record.
     *
     * @return true if this method processes a record, false if it does not process a record.
     * @throws TaskMigratedException if the task producer got fenced (EOS only)
     */
    @SuppressWarnings("unchecked")
    public boolean process() {
        // get the next record to process
        final StampedRecord record = partitionGroup.nextRecord(recordInfo);

        // if there is no record to process, return immediately
        if (record == null) {
            return false;
        }

        try {
            // process the record by passing to the source node of the topology
            final ProcessorNode currNode = recordInfo.node();
            final TopicPartition partition = recordInfo.partition();

            log.trace("Start processing one record [{}]", record);

            updateProcessorContext(record, currNode);
            maybeMeasureLatency(() -> currNode.process(record.key(), record.value()), time, processLatencySensor);

            log.trace("Completed processing one record [{}]", record);

            // update the consumed offset map after processing is done
            consumedOffsets.put(partition, record.offset());
            commitNeeded = true;

            // after processing this record, if its partition queue's buffered size has been
            // decreased to the threshold, we can then resume the consumption on this partition
            if (recordInfo.queue().size() == maxBufferedSize) {
                consumer.resume(singleton(partition));
            }
        } catch (final RecoverableClientException e) {
            throw new TaskMigratedException(this, e);
        } catch (final KafkaException e) {
            final String stackTrace = getStacktraceString(e);
            throw new StreamsException(format("Exception caught in process. taskId=%s, " +
                    "processor=%s, topic=%s, partition=%d, offset=%d, stacktrace=%s",
                id(),
                processorContext.currentNode().name(),
                record.topic(),
                record.partition(),
                record.offset(),
                stackTrace
            ), e);
        } finally {
            processorContext.setCurrentNode(null);
        }

        return true;
    }

    private String getStacktraceString(final KafkaException e) {
        String stacktrace = null;
        try (final StringWriter stringWriter = new StringWriter();
             final PrintWriter printWriter = new PrintWriter(stringWriter)) {
            e.printStackTrace(printWriter);
            stacktrace = stringWriter.toString();
        } catch (final IOException ioe) {
            log.error("Encountered error extracting stacktrace from this exception", ioe);
        }
        return stacktrace;
    }

    /**
     * @throws IllegalStateException if the current node is not null
     * @throws TaskMigratedException if the task producer got fenced (EOS only)
     */
    @Override
    public void punctuate(final ProcessorNode node, final long timestamp, final PunctuationType type, final Punctuator punctuator) {
        if (processorContext.currentNode() != null) {
            throw new IllegalStateException(String.format("%sCurrent node is not null", logPrefix));
        }

        updateProcessorContext(new StampedRecord(DUMMY_RECORD, timestamp), node);

        if (log.isTraceEnabled()) {
            log.trace("Punctuating processor {} with timestamp {} and punctuation type {}", node.name(), timestamp, type);
        }

        try {
            maybeMeasureLatency(() -> node.punctuate(timestamp, punctuator), time, punctuateLatencySensor);
        } catch (final RecoverableClientException e) {
            throw new TaskMigratedException(this, e);
        } catch (final KafkaException e) {
            throw new StreamsException(String.format("%sException caught while punctuating processor '%s'", logPrefix, node.name()), e);
        } finally {
            processorContext.setCurrentNode(null);
        }
    }

    private void updateProcessorContext(final StampedRecord record, final ProcessorNode currNode) {
        processorContext.setRecordContext(
            new ProcessorRecordContext(
                record.timestamp,
                record.offset(),
                record.partition(),
                record.topic(),
                record.headers()));
        processorContext.setCurrentNode(currNode);
    }

    /**
     * <pre>
     * - flush state and producer
     * - if(!eos) write checkpoint
     * - commit offsets and start new transaction
     * </pre>
     *
     * @throws TaskMigratedException if committing offsets failed (non-EOS)
     *                               or if the task producer got fenced (EOS)
     */
    @Override
    public void commit() {
        commit(true, extractPartitionTimes());
    }

    /**
     * @throws TaskMigratedException if committing offsets failed (non-EOS)
     *                               or if the task producer got fenced (EOS)
     */
    // visible for testing
    void commit(final boolean startNewTransaction, final Map<TopicPartition, Long> partitionTimes) {
        final long startNs = time.nanoseconds();
        log.debug("Committing");

        flushState();

        if (!eosEnabled) {
            stateMgr.checkpoint(activeTaskCheckpointableOffsets());
        }

        final Map<TopicPartition, OffsetAndMetadata> consumedOffsetsAndMetadata = new HashMap<>(consumedOffsets.size());
        for (final Map.Entry<TopicPartition, Long> entry : consumedOffsets.entrySet()) {
            final TopicPartition partition = entry.getKey();
            final long offset = entry.getValue() + 1;
            final long partitionTime = partitionTimes.get(partition);
            consumedOffsetsAndMetadata.put(partition, new OffsetAndMetadata(offset, encodeTimestamp(partitionTime)));
        }

        try {
            if (eosEnabled) {
                producer.sendOffsetsToTransaction(consumedOffsetsAndMetadata, applicationId);
                producer.commitTransaction();
                transactionInFlight = false;
                if (startNewTransaction) {
                    producer.beginTransaction();
                    transactionInFlight = true;
                }
            } else {
                consumer.commitSync(consumedOffsetsAndMetadata);
            }
        } catch (final CommitFailedException | ProducerFencedException | UnknownProducerIdException error) {
            throw new TaskMigratedException(this, error);
        } catch (final RebalanceInProgressException error) {
            // commitSync throws this error and can be ignored (since EOS is not enabled, even if the task crashed
            // immediately after this commit, we would just reprocess those records again)
            log.info("Committing failed with a non-fatal error: {}, " +
                "we can ignore this since commit may succeed still",  error.toString());
        }

        commitNeeded = false;
        commitRequested = false;
        commitSensor.record(time.nanoseconds() - startNs);
    }

    private Map<TopicPartition, Long> activeTaskCheckpointableOffsets() {
        final Map<TopicPartition, Long> checkpointableOffsets = new HashMap<>(recordCollector.offsets());
        for (final Map.Entry<TopicPartition, Long> entry : consumedOffsets.entrySet()) {
            checkpointableOffsets.putIfAbsent(entry.getKey(), entry.getValue());
        }
        return checkpointableOffsets;
    }

    @Override
    protected void flushState() {
        log.trace("Flushing state and producer");
        super.flushState();
        try {
            recordCollector.flush();
        } catch (final RecoverableClientException e) {
            throw new TaskMigratedException(this, e);
        }
    }

    Map<TopicPartition, Long> purgableOffsets() {
        final Map<TopicPartition, Long> purgableConsumedOffsets = new HashMap<>();
        for (final Map.Entry<TopicPartition, Long> entry : consumedOffsets.entrySet()) {
            final TopicPartition tp = entry.getKey();
            if (topology.isRepartitionTopic(tp.topic())) {
                purgableConsumedOffsets.put(tp, entry.getValue() + 1);
            }
        }

        return purgableConsumedOffsets;
    }

    private void initTopology() {
        // initialize the task by initializing all its processor nodes in the topology
        log.trace("Initializing processor nodes of the topology");
        for (final ProcessorNode node : topology.processors()) {
            processorContext.setCurrentNode(node);
            try {
                node.init(processorContext);
            } finally {
                processorContext.setCurrentNode(null);
            }
        }
    }

    /**
     * <pre>
     * - close topology
     * - {@link #commit()}
     *   - flush state and producer
     *   - if (!eos) write checkpoint
     *   - commit offsets
     * </pre>
     *
     * @throws TaskMigratedException if committing offsets failed (non-EOS)
     *                               or if the task producer got fenced (EOS)
     */
    public void suspend() {
        log.debug("Suspending");
        suspend(true, false);
    }

    /**
     * <pre>
     * - close topology
     * - if (clean) {@link #commit()}
     *   - flush state and producer
     *   - if (!eos) write checkpoint
     *   - commit offsets
     * </pre>
     *
     * @throws TaskMigratedException if committing offsets failed (non-EOS)
     *                               or if the task producer got fenced (EOS)
     */
    // visible for testing
    void suspend(final boolean clean,
                 final boolean isZombie) {
        // this is necessary because all partition times are reset to -1 during close
        // we need to preserve the original partitions times before calling commit
        final Map<TopicPartition, Long> partitionTimes = extractPartitionTimes();
        try {
            closeTopology(); // should we call this only on clean suspend?
        } catch (final RuntimeException fatal) {
            if (clean) {
                throw fatal;
            }
        }

        if (clean) {
            TaskMigratedException taskMigratedException = null;
            try {
                commit(false, partitionTimes);
            } finally {
                if (eosEnabled) {
                    stateMgr.checkpoint(activeTaskCheckpointableOffsets());

                    try {
                        recordCollector.close();
                    } catch (final RecoverableClientException e) {
                        taskMigratedException = new TaskMigratedException(this, e);
                    } finally {
                        producer = null;
                    }
                }
            }
            if (taskMigratedException != null) {
                throw taskMigratedException;
            }
        } else {
            // In the case of unclean close we still need to make sure all the stores are flushed before closing any
            super.flushState();

            if (eosEnabled) {
                maybeAbortTransactionAndCloseRecordCollector(isZombie);
            }
        }
    }

    private void maybeAbortTransactionAndCloseRecordCollector(final boolean isZombie) {
        if (!isZombie) {
            try {
                if (transactionInFlight) {
                    producer.abortTransaction();
                }
                transactionInFlight = false;
            } catch (final ProducerFencedException ignore) {
                /* TODO
                 * this should actually never happen atm as we guard the call to #abortTransaction
                 * -> the reason for the guard is a "bug" in the Producer -- it throws IllegalStateException
                 * instead of ProducerFencedException atm. We can remove the isZombie flag after KAFKA-5604 got
                 * fixed and fall-back to this catch-and-swallow code
                 */

                // can be ignored: transaction got already aborted by brokers/transactional-coordinator if this happens
            }
        }

        try {
            recordCollector.close();
        } catch (final Throwable e) {
            log.error("Failed to close producer due to the following error:", e);
        } finally {
            producer = null;
        }
    }

    private void closeTopology() {
        log.trace("Closing processor topology");

        partitionGroup.clear();

        // close the processors
        // make sure close() is called for each node even when there is a RuntimeException
        RuntimeException exception = null;
        if (taskInitialized) {
            for (final ProcessorNode node : topology.processors()) {
                processorContext.setCurrentNode(node);
                try {
                    node.close();
                } catch (final RuntimeException e) {
                    exception = e;
                } finally {
                    processorContext.setCurrentNode(null);
                }
            }
        }

        if (exception != null) {
            throw exception;
        }
    }

    // helper to avoid calling suspend() twice if a suspended task is not reassigned and closed
    void closeSuspended(final boolean clean, RuntimeException firstException) {
        try {
            closeStateManager(clean);
        } catch (final RuntimeException e) {
            if (firstException == null) {
                firstException = e;
            }
            log.error("Could not close state manager due to the following error:", e);
        }

        partitionGroup.close();
        streamsMetrics.removeAllTaskLevelSensors(threadId, id.toString());

        closeTaskSensor.record();

        if (firstException != null) {
            throw firstException;
        }
    }

    /**
     * <pre>
     * - {@link #suspend(boolean, boolean) suspend(clean)}
     *   - close topology
     *   - if (clean) {@link #commit()}
     *     - flush state and producer
     *     - commit offsets
     * - close state
     *   - if (clean) write checkpoint
     * - if (eos) close producer
     * </pre>
     *
     * @param clean    shut down cleanly (ie, incl. flush and commit) if {@code true} --
     *                 otherwise, just close open resources
     * @param isZombie {@code true} is this task is a zombie or not (this will repress {@link TaskMigratedException}
     * @throws TaskMigratedException if committing offsets failed (non-EOS)
     *                               or if the task producer got fenced (EOS)
     */
    @Override
    public void close(boolean clean,
                      final boolean isZombie) {
        log.debug("Closing");

        RuntimeException firstException = null;
        try {
            suspend(clean, isZombie);
        } catch (final RuntimeException e) {
            clean = false;
            firstException = e;
            log.error("Could not close task due to the following error:", e);
        }

        closeSuspended(clean, firstException);

        taskClosed = true;
    }

    /**
     * Adds records to queues. If a record has an invalid (i.e., negative) timestamp, the record is skipped
     * and not added to the queue for processing
     *
     * @param partition the partition
     * @param records   the records
     */
    public void addRecords(final TopicPartition partition, final Iterable<ConsumerRecord<byte[], byte[]>> records) {
        final int newQueueSize = partitionGroup.addRawRecords(partition, records);

        if (log.isTraceEnabled()) {
            log.trace("Added records into the buffered queue of partition {}, new queue size is {}", partition, newQueueSize);
        }

        // if after adding these records, its partition queue's buffered size has been
        // increased beyond the threshold, we can then pause the consumption for this partition
        if (newQueueSize > maxBufferedSize) {
            consumer.pause(singleton(partition));
        }
    }

    /**
     * Schedules a punctuation for the processor
     *
     * @param interval the interval in milliseconds
     * @param type     the punctuation type
     * @throws IllegalStateException if the current node is not null
     */
    public Cancellable schedule(final long interval, final PunctuationType type, final Punctuator punctuator) {
        switch (type) {
            case STREAM_TIME:
                // align punctuation to 0L, punctuate as soon as we have data
                return schedule(0L, interval, type, punctuator);
            case WALL_CLOCK_TIME:
                // align punctuation to now, punctuate after interval has elapsed
                return schedule(time.milliseconds() + interval, interval, type, punctuator);
            default:
                throw new IllegalArgumentException("Unrecognized PunctuationType: " + type);
        }
    }

    /**
     * Schedules a punctuation for the processor
     *
     * @param startTime time of the first punctuation
     * @param interval  the interval in milliseconds
     * @param type      the punctuation type
     * @throws IllegalStateException if the current node is not null
     */
    Cancellable schedule(final long startTime, final long interval, final PunctuationType type, final Punctuator punctuator) {
        if (processorContext.currentNode() == null) {
            throw new IllegalStateException(String.format("%sCurrent node is null", logPrefix));
        }

        final PunctuationSchedule schedule = new PunctuationSchedule(processorContext.currentNode(), startTime, interval, punctuator);

        switch (type) {
            case STREAM_TIME:
                // STREAM_TIME punctuation is data driven, will first punctuate as soon as stream-time is known and >= time,
                // stream-time is known when we have received at least one record from each input topic
                return streamTimePunctuationQueue.schedule(schedule);
            case WALL_CLOCK_TIME:
                // WALL_CLOCK_TIME is driven by the wall clock time, will first punctuate when now >= time
                return systemTimePunctuationQueue.schedule(schedule);
            default:
                throw new IllegalArgumentException("Unrecognized PunctuationType: " + type);
        }
    }

    /**
     * @return The number of records left in the buffer of this task's partition group
     */
    int numBuffered() {
        return partitionGroup.numBuffered();
    }

    /**
     * Possibly trigger registered stream-time punctuation functions if
     * current partition group timestamp has reached the defined stamp
     * Note, this is only called in the presence of new records
     *
     * @throws TaskMigratedException if the task producer got fenced (EOS only)
     */
    public boolean maybePunctuateStreamTime() {
        final long streamTime = partitionGroup.streamTime();

        // if the timestamp is not known yet, meaning there is not enough data accumulated
        // to reason stream partition time, then skip.
        if (streamTime == RecordQueue.UNKNOWN) {
            return false;
        } else {
            final boolean punctuated = streamTimePunctuationQueue.mayPunctuate(streamTime, PunctuationType.STREAM_TIME, this);

            if (punctuated) {
                commitNeeded = true;
            }

            return punctuated;
        }
    }

    /**
     * Possibly trigger registered system-time punctuation functions if
     * current system timestamp has reached the defined stamp
     * Note, this is called irrespective of the presence of new records
     *
     * @throws TaskMigratedException if the task producer got fenced (EOS only)
     */
    public boolean maybePunctuateSystemTime() {
        final long systemTime = time.milliseconds();

        final boolean punctuated = systemTimePunctuationQueue.mayPunctuate(systemTime, PunctuationType.WALL_CLOCK_TIME, this);

        if (punctuated) {
            commitNeeded = true;
        }

        return punctuated;
    }

    /**
     * Request committing the current task's state
     */
    void requestCommit() {
        commitRequested = true;
    }

    /**
     * Whether or not a request has been made to commit the current state
     */
    boolean commitRequested() {
        return commitRequested;
    }

    // visible for testing only
    RecordCollector recordCollector() {
        return recordCollector;
    }

    // used for testing
    long streamTime() {
        return partitionGroup.streamTime();
    }

    // used for testing
    long partitionTime(final TopicPartition partition) {
        return partitionGroup.partitionTimestamp(partition);
    }

    Producer<byte[], byte[]> getProducer() {
        return producer;
    }

    private void initializeTransactions() {
        try {
            producer.initTransactions();
        } catch (final TimeoutException retriable) {
            log.error(
                "Timeout exception caught when initializing transactions for task {}. " +
                    "This might happen if the broker is slow to respond, if the network connection to " +
                    "the broker was interrupted, or if similar circumstances arise. " +
                    "You can increase producer parameter `max.block.ms` to increase this timeout.",
                id,
                retriable
            );
            throw new StreamsException(
                format("%sFailed to initialize task %s due to timeout.", logPrefix, id),
                retriable
            );
        }
    }

    // visible for testing
    String encodeTimestamp(final long partitionTime) {
        final ByteBuffer buffer = ByteBuffer.allocate(9);
        buffer.put(LATEST_MAGIC_BYTE);
        buffer.putLong(partitionTime);
        return Base64.getEncoder().encodeToString(buffer.array());
    }

    // visible for testing
    long decodeTimestamp(final String encryptedString) {
        if (encryptedString.length() == 0) {
            return RecordQueue.UNKNOWN;
        }
        final ByteBuffer buffer = ByteBuffer.wrap(Base64.getDecoder().decode(encryptedString));
        final byte version = buffer.get();
        switch (version) {
            case LATEST_MAGIC_BYTE:
                return buffer.getLong();
            default: 
                log.warn("Unsupported offset metadata version found. Supported version {}. Found version {}.", 
                         LATEST_MAGIC_BYTE, version);
                return RecordQueue.UNKNOWN;
        }
    }

    private Map<TopicPartition, Long> extractPartitionTimes() {
        final Map<TopicPartition, Long> partitionTimes = new HashMap<>();
        for (final TopicPartition partition : partitionGroup.partitions()) {
            partitionTimes.put(partition, partitionTime(partition));
        }
        return partitionTimes;
    }

    public Map<TopicPartition, Long> restoredOffsets() {
        return stateMgr.changelogReader().restoredOffsets();
    }
}
