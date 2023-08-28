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

package kafka.log.s3;

import kafka.log.s3.model.StreamRecordBatch;
import kafka.log.s3.objects.ObjectManager;
import kafka.log.s3.operator.S3Operator;
import org.apache.kafka.common.utils.ThreadUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Queue;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

public class S3Wal implements Wal {
    private static final Logger LOGGER = LoggerFactory.getLogger(S3Wal.class);
    private final int batchIntervalMs = 200;
    private final BlockingQueue<WalWriteRequest> writeBuffer;
    private final WalBatchWriteTask walBatchWriteTask;
    private final MinorCompactTask minorCompactTask;


    public S3Wal(ObjectManager objectManager, S3Operator s3Operator) {
        writeBuffer = new ArrayBlockingQueue<>(16384);
        walBatchWriteTask = new WalBatchWriteTask(objectManager, s3Operator);
        minorCompactTask = new MinorCompactTask(5L * 1024 * 1024 * 1024, 60, 16 * 1024 * 1024, objectManager, s3Operator);
    }

    @Override
    public void close() {
        walBatchWriteTask.close();
    }

    @Override
    public CompletableFuture<Void> append(StreamRecordBatch streamRecord) {
        CompletableFuture<Void> cf = new CompletableFuture<>();
        //TODO: copy to pooled bytebuffer to reduce gc, convert to flat record
        try {
            writeBuffer.put(new WalWriteRequest(streamRecord, cf));
        } catch (InterruptedException e) {
            cf.completeExceptionally(e);
        }
        return cf;
    }

    class WalBatchWriteTask implements Runnable {
        private final ScheduledExecutorService schedule = Executors.newSingleThreadScheduledExecutor(
                ThreadUtils.createThreadFactory("wal-batch-write", true));
        private final Queue<SingleWalObjectWriteTask> writeTasks = new ConcurrentLinkedQueue<>();
        private final ObjectManager objectManager;
        private final S3Operator s3Operator;

        public WalBatchWriteTask(ObjectManager objectManager, S3Operator s3Operator) {
            this.objectManager = objectManager;
            this.s3Operator = s3Operator;
            schedule.scheduleAtFixedRate(this, batchIntervalMs, batchIntervalMs, TimeUnit.MILLISECONDS);
        }

        public void close() {
            schedule.shutdown();
            run();
        }

        @Override
        public void run() {
            try {
                run0();
            } catch (Throwable e) {
                LOGGER.error("Error in wal batch write task", e);
            }
        }

        void run0() {
            List<WalWriteRequest> requests = new ArrayList<>(writeBuffer.size());
            writeBuffer.drainTo(requests);
            if (requests.isEmpty()) {
                return;
            }
            SingleWalObjectWriteTask singleWalObjectWriteTask = new SingleWalObjectWriteTask(requests, objectManager, s3Operator);
            writeTasks.offer(singleWalObjectWriteTask);
            runWriteTask(singleWalObjectWriteTask);
        }

        void runWriteTask(SingleWalObjectWriteTask task) {
            task.upload().thenAccept(nil -> schedule.execute(this::tryComplete))
                    .exceptionally(ex -> {
                        LOGGER.warn("Write wal object fail, retry later", ex);
                        schedule.schedule(() -> runWriteTask(task), batchIntervalMs, TimeUnit.MILLISECONDS);
                        return null;
                    });
        }

        void tryComplete() {
            while (true) {
                SingleWalObjectWriteTask task = writeTasks.peek();
                if (task == null) {
                    return;
                }
                if (task.isDone()) {
                    writeTasks.poll();
                    task.ack();
                    minorCompactTask.tryCompact(new MinorCompactTask.MinorCompactPart(task.objectId(), task.records()));
                } else {
                    return;
                }
            }
        }
    }
}
