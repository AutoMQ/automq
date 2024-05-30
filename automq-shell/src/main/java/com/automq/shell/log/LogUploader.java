/*
 * Copyright 2024, AutoMQ CO.,LTD.
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
import com.automq.shell.auth.CredentialsProviderHolder;
import com.automq.stream.s3.network.ThrottleStrategy;
import com.automq.stream.s3.operator.DefaultS3Operator;
import com.automq.stream.s3.operator.S3Operator;
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
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class LogUploader implements LogRecorder {
    private static final Logger LOGGER = LoggerFactory.getLogger(LogUploader.class);

    public static final int DEFAULT_MAX_QUEUE_SIZE = 64 * 1024;
    public static final int DEFAULT_BUFFER_SIZE = 16 * 1024 * 1024;
    public static final int MAX_UPLOAD_INTERVAL = 2 * 60 * 1000;
    public static final int MAX_JITTER_INTERVAL = 60 * 1000;

    private static final LogUploader INSTANCE = new LogUploader();

    private final BlockingQueue<LogEvent> queue = new LinkedBlockingQueue<>(DEFAULT_MAX_QUEUE_SIZE);
    private final ByteBuf uploadBuffer = Unpooled.directBuffer(DEFAULT_BUFFER_SIZE);
    private final Random random = new Random();
    private volatile long lastUploadTimestamp = 0L;
    private volatile long nextUploadInterval = MAX_UPLOAD_INTERVAL + random.nextInt(MAX_JITTER_INTERVAL);

    private volatile boolean closed;

    private volatile S3LogConfig config;

    private volatile CompletableFuture<Void> startFuture;
    private S3Operator s3Operator;
    private Thread uploadThread;
    private Thread cleanupThread;

    private LogUploader() {
    }

    public static LogUploader getInstance() {
        return INSTANCE;
    }

    public void close() {
        closed = true;
        if (uploadThread != null) {
            uploadThread.interrupt();
            cleanupThread.interrupt();

            if (uploadBuffer.readableBytes() > 0) {
                try {
                    s3Operator.write(getObjectKey(), uploadBuffer, ThrottleStrategy.BYPASS).get();
                } catch (Exception ignore) {
                }
            }
            s3Operator.close();
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
        boolean enabled = config != null && config.isEnabled() && StringUtils.isNotBlank(config.s3OpsBucket());

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
                            s3Operator = new DefaultS3Operator(config.s3Endpoint(), config.s3Region(),
                                config.s3OpsBucket(), config.s3PathStyle(), List.of(CredentialsProviderHolder.getAwsCredentialsProvider()), false);
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
                        Runtime.getRuntime().addShutdownHook(new Thread(this::close));
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
                    } else if (closed) {
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
                        while (!closed && !Thread.currentThread().isInterrupted()) {
                            if (s3Operator == null) {
                                break;
                            }

                            try {
                                String objectKey = getObjectKey();
                                s3Operator.write(objectKey, uploadBuffer.retainedSlice().asReadOnly(), ThrottleStrategy.BYPASS).get();
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
                nextUploadInterval = MAX_UPLOAD_INTERVAL + random.nextInt(MAX_JITTER_INTERVAL);
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
                    long expiredTime = System.currentTimeMillis() - MAX_UPLOAD_INTERVAL;

                    List<Pair<String, Long>> pairList = s3Operator.list(String.format("automq/logs/%s", config.clusterId())).join();

                    if (!pairList.isEmpty()) {
                        List<String> keyList = pairList.stream()
                            .filter(pair -> pair.getRight() < expiredTime)
                            .map(Pair::getLeft)
                            .collect(Collectors.toList());
                        s3Operator.delete(keyList).join();
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
