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

package com.automq.shell.log;

import com.automq.shell.AutoMQApplication;
import com.automq.stream.s3.operator.ObjectStorage;
import com.automq.stream.s3.operator.ObjectStorage.ObjectInfo;
import com.automq.stream.s3.operator.ObjectStorage.ObjectPath;
import com.automq.stream.s3.operator.ObjectStorage.WriteOptions;
import com.google.common.collect.Lists;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class LogUploader implements LogRecorder {
    private static final Logger LOGGER = LoggerFactory.getLogger(LogUploader.class);

    public static final int DEFAULT_MAX_QUEUE_SIZE = 64 * 1024;
    public static final int DEFAULT_BUFFER_SIZE = 16 * 1024 * 1024;
    public static final int UPLOAD_INTERVAL = System.getenv("AUTOMQ_OBSERVABILITY_UPLOAD_INTERVAL") != null ? Integer.parseInt(System.getenv("AUTOMQ_OBSERVABILITY_UPLOAD_INTERVAL")) : 60 * 1000;
    public static final int CLEANUP_INTERVAL = System.getenv("AUTOMQ_OBSERVABILITY_CLEANUP_INTERVAL") != null ? Integer.parseInt(System.getenv("AUTOMQ_OBSERVABILITY_CLEANUP_INTERVAL")) : 2 * 60 * 1000;
    public static final int MAX_JITTER_INTERVAL = 60 * 1000;

    private static final LogUploader INSTANCE = new LogUploader();

    private final BlockingQueue<LogEvent> queue = new LinkedBlockingQueue<>(DEFAULT_MAX_QUEUE_SIZE);
    private final ByteBuf uploadBuffer = Unpooled.directBuffer(DEFAULT_BUFFER_SIZE);
    private final Random random = new Random();
    private volatile long lastUploadTimestamp = System.currentTimeMillis();
    private volatile long nextUploadInterval = UPLOAD_INTERVAL + random.nextInt(MAX_JITTER_INTERVAL);

    private volatile boolean closed;

    private volatile S3LogConfig config;

    private volatile CompletableFuture<Void> startFuture;
    private ObjectStorage objectStorage;
    private Thread uploadThread;
    private Thread cleanupThread;

    private LogUploader() {
    }

    public static LogUploader getInstance() {
        return INSTANCE;
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
        if (!closed && couldUpload()) {
            return queue.offer(event);
        }
        return false;
    }

    private boolean couldUpload() {
        initConfiguration();
        boolean enabled = config != null && config.isEnabled() && config.objectStorage() != null;

        if (enabled) {
            initUploadComponent();
        }

        return enabled && startFuture != null && startFuture.isDone();
    }

    private void initConfiguration() {
        if (config == null) {
            synchronized (this) {
                if (config == null) {
                    config = AutoMQApplication.getBean(S3LogConfig.class);
                }
            }
        }
    }

    private void initUploadComponent() {
        if (startFuture == null) {
            synchronized (this) {
                if (startFuture == null) {
                    startFuture = CompletableFuture.runAsync(() -> {
                        try {
                            objectStorage = config.objectStorage();
                            uploadThread = new Thread(new UploadTask());
                            uploadThread.setName("log-uploader-upload-thread");
                            uploadThread.setDaemon(true);
                            uploadThread.start();

                            cleanupThread = new Thread(new CleanupTask());
                            cleanupThread.setName("log-uploader-cleanup-thread");
                            cleanupThread.setDaemon(true);
                            cleanupThread.start();

                            startFuture.complete(null);
                        } catch (Exception e) {
                            LOGGER.error("Initialize log uploader failed", e);
                        }
                    }, command -> new Thread(command).start());
                }
            }
        }
    }

    private class UploadTask implements Runnable {

        public String formatTimestampInMillis(long timestamp) {
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
                        // DateTime Level [Logger] Message \n stackTrace
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
                if (couldUpload()) {
                    try {
                        while (!Thread.currentThread().isInterrupted()) {
                            if (objectStorage == null) {
                                break;
                            }

                            try {
                                String objectKey = getObjectKey();
                                objectStorage.write(WriteOptions.DEFAULT, objectKey, uploadBuffer.retainedSlice().asReadOnly()).get();
                                break;
                            } catch (Exception e) {
                                e.printStackTrace(System.err);
                                Thread.sleep(1000);
                            }
                        }
                    } catch (InterruptedException e) {
                        //ignore
                    }
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
                    if (closed || !config.isActiveController()) {
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
                            // Some of s3 implements allow only 1000 keys per request.
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
