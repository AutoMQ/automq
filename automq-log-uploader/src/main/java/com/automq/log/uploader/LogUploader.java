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

package com.automq.log.uploader;

import com.automq.log.uploader.util.Utils;
import com.automq.stream.s3.operator.ObjectStorage;
import com.automq.stream.s3.operator.ObjectStorage.ObjectInfo;
import com.automq.stream.s3.operator.ObjectStorage.ObjectPath;
import com.automq.stream.s3.operator.ObjectStorage.WriteOptions;
import com.google.common.collect.Lists;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.List;
import java.util.Random;
import java.util.UUID;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;

public class LogUploader implements LogRecorder {
    private static final Logger LOGGER = LoggerFactory.getLogger(LogUploader.class);

    public static final int DEFAULT_MAX_QUEUE_SIZE = 64 * 1024;
    public static final int DEFAULT_BUFFER_SIZE = 16 * 1024 * 1024;
    public static final int UPLOAD_INTERVAL = System.getenv("AUTOMQ_OBSERVABILITY_UPLOAD_INTERVAL") != null
        ? Integer.parseInt(System.getenv("AUTOMQ_OBSERVABILITY_UPLOAD_INTERVAL"))
        : 60 * 1000;
    public static final int CLEANUP_INTERVAL = System.getenv("AUTOMQ_OBSERVABILITY_CLEANUP_INTERVAL") != null
        ? Integer.parseInt(System.getenv("AUTOMQ_OBSERVABILITY_CLEANUP_INTERVAL"))
        : 2 * 60 * 1000;
    public static final int MAX_JITTER_INTERVAL = 60 * 1000;

    private final BlockingQueue<LogEvent> queue = new LinkedBlockingQueue<>(DEFAULT_MAX_QUEUE_SIZE);
    private final ByteBuf uploadBuffer = Unpooled.directBuffer(DEFAULT_BUFFER_SIZE);
    private final Random random = new Random();
    private volatile long lastUploadTimestamp = System.currentTimeMillis();
    private volatile long nextUploadInterval = UPLOAD_INTERVAL + random.nextInt(MAX_JITTER_INTERVAL);

    private volatile boolean closed;

    private volatile S3LogConfig config;

    private ObjectStorage objectStorage;
    private Thread uploadThread;
    private Thread cleanupThread;

    public LogUploader() {
    }

    public synchronized void start(S3LogConfig config) {
        if (this.config != null) {
            LOGGER.warn("LogUploader is already started.");
            return;
        }
        this.config = config;
        if (!config.isEnabled() || config.objectStorage() == null) {
            LOGGER.warn("LogUploader is disabled due to configuration.");
            closed = true;
            return;
        }

        try {
            this.objectStorage = config.objectStorage();
            this.uploadThread = new Thread(new UploadTask());
            this.uploadThread.setName("log-uploader-upload-thread");
            this.uploadThread.setDaemon(true);
            this.uploadThread.start();

            this.cleanupThread = new Thread(new CleanupTask());
            this.cleanupThread.setName("log-uploader-cleanup-thread");
            this.cleanupThread.setDaemon(true);
            this.cleanupThread.start();

            LOGGER.info("LogUploader started successfully.");
        } catch (Exception e) {
            LOGGER.error("Failed to start LogUploader", e);
            closed = true;
        }
    }

    public void close() throws InterruptedException {
        closed = true;
        if (uploadThread != null) {
            uploadThread.join();
            objectStorage.close();
        }

        if (cleanupThread != null) {
            cleanupThread.interrupt();
        }
    }

    @Override
    public boolean append(LogEvent event) {
        if (!closed) {
            return queue.offer(event);
        }
        return false;
    }

    private class UploadTask implements Runnable {

        private String formatTimestampInMillis(long timestamp) {
            return ZonedDateTime.ofInstant(Instant.ofEpochMilli(timestamp), ZoneId.systemDefault())
                .format(DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.SSS Z"));
        }

        @Override
        public void run() {
            while (!Thread.currentThread().isInterrupted()) {
                try {
                    long now = System.currentTimeMillis();
                    LogEvent event = queue.poll(1, TimeUnit.SECONDS);
                    if (event != null) {
                        StringBuilder logLine = new StringBuilder()
                            .append(formatTimestampInMillis(event.timestampMillis()))
                            .append(" ")
                            .append(event.level())
                            .append(" ")
                            .append("[").append(event.logger()).append("] ")
                            .append(" ")
                            .append(event.message())
                            .append("\n");

                        String[] throwableStrRep = event.stackTrace();
                        if (throwableStrRep != null) {
                            for (String stack : throwableStrRep) {
                                logLine.append(stack).append("\n");
                            }
                        }

                        byte[] bytes = logLine.toString().getBytes(StandardCharsets.UTF_8);
                        if (uploadBuffer.writableBytes() < bytes.length || now - lastUploadTimestamp > nextUploadInterval) {
                            upload(now);
                        }
                        uploadBuffer.writeBytes(bytes);
                    } else if (closed && queue.isEmpty()) {
                        upload(now);
                        break;
                    } else if (now - lastUploadTimestamp > nextUploadInterval) {
                        upload(now);
                    }
                } catch (InterruptedException e) {
                    break;
                } catch (Exception e) {
                    LOGGER.error("Upload log to s3 failed", e);
                }
            }
        }

        private void upload(long now) {
            if (uploadBuffer.readableBytes() > 0) {
                try {
                    while (!Thread.currentThread().isInterrupted()) {
                        if (objectStorage == null) {
                            break;
                        }
                        try {
                            String objectKey = getObjectKey();
                            objectStorage.write(WriteOptions.DEFAULT, objectKey, Utils.compress(uploadBuffer.slice().asReadOnly())).get();
                            break;
                        } catch (Exception e) {
                            LOGGER.warn("Failed to upload logs, will retry", e);
                            Thread.sleep(1000);
                        }
                    }
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
                uploadBuffer.clear();
                lastUploadTimestamp = now;
                nextUploadInterval = UPLOAD_INTERVAL + random.nextInt(MAX_JITTER_INTERVAL);
            }
        }
    }

    private class CleanupTask implements Runnable {

        @Override
        public void run() {
            while (!Thread.currentThread().isInterrupted()) {
                try {
                    if (closed || !config.isLeader()) {
                        Thread.sleep(Duration.ofMinutes(1).toMillis());
                        continue;
                    }
                    long expiredTime = System.currentTimeMillis() - CLEANUP_INTERVAL;
                    List<ObjectInfo> objects = objectStorage.list(String.format("automq/logs/%s", config.clusterId())).join();

                    if (!objects.isEmpty()) {
                        List<ObjectPath> keyList = objects.stream()
                            .filter(object -> object.timestamp() < expiredTime)
                            .map(object -> new ObjectPath(object.bucketId(), object.key()))
                            .collect(Collectors.toList());

                        if (!keyList.isEmpty()) {
                            CompletableFuture<?>[] deleteFutures = Lists.partition(keyList, 1000)
                                .stream()
                                .map(objectStorage::delete)
                                .toArray(CompletableFuture[]::new);
                            CompletableFuture.allOf(deleteFutures).join();
                        }
                    }
                    Thread.sleep(Duration.ofMinutes(1).toMillis());
                } catch (InterruptedException e) {
                    break;
                } catch (Exception e) {
                    LOGGER.error("Cleanup s3 logs failed", e);
                }
            }
        }
    }

    private String getObjectKey() {
        String hour = LocalDateTime.now(ZoneOffset.UTC).format(DateTimeFormatter.ofPattern("yyyyMMddHH"));
        return String.format("automq/logs/%s/%s/%s/%s", config.clusterId(), config.nodeId(), hour, UUID.randomUUID());
    }
}
