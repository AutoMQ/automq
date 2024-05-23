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
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import org.apache.log4j.RollingFileAppender;
import org.apache.log4j.spi.LoggingEvent;

public class S3RollingFileAppender extends RollingFileAppender {
    public static final int DEFAULT_MAX_QUEUE_SIZE = 64 * 1024;
    public static final int DEFAULT_BUFFER_SIZE = 64 * 1024 * 1024;
    public static final int MAX_UPLOAD_INTERVAL = 30 * 1000;

    private final BlockingQueue<LoggingEvent> queue = new LinkedBlockingQueue<>(DEFAULT_MAX_QUEUE_SIZE);
    private final ByteBuf uploadBuffer = Unpooled.directBuffer(DEFAULT_BUFFER_SIZE);
    private volatile long lastUploadTimestamp = 0L;

    private volatile S3LogConfig config;
    private volatile String clusterId;

    private volatile CompletableFuture<Void> startFuture;
    private S3Operator s3Operator;
    private Thread uploadThread;

    @Override
    public void close() {
        super.close();
        if (uploadThread != null) {
            uploadThread.interrupt();
            s3Operator.close();
        }
    }

    private void initConfiguration() {
        if (clusterId == null) {
            synchronized (this) {
                if (clusterId == null) {
                    clusterId = AutoMQApplication.getClusterId();
                }
            }
        }

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
                            // TODO: use separated bucket.
                            s3Operator = new DefaultS3Operator(config.s3Endpoint(), config.s3Region(),
                                config.s3Bucket(), config.s3PathStyle(), List.of(CredentialsProviderHolder.getAwsCredentialsProvider()), false);
                            uploadThread = new Thread(new UploadTask());
                            uploadThread.start();
                            startFuture.complete(null);
                        } catch (Exception e) {
                            e.printStackTrace(System.err);
                        }
                    });
                }
            }
        }
    }

    private boolean couldUpload() {
        initConfiguration();
        // TODO: use separated switch.
        boolean enabled = clusterId != null && config != null && config.isDebugEnabled();

        if (enabled) {
            initUploadComponent();
        }

        return enabled && startFuture != null && startFuture.isDone();
    }

    @Override
    protected void subAppend(LoggingEvent event) {
        super.subAppend(event);
        if (!closed && couldUpload()) {
            queue.offer(event);
        }
    }

    private class UploadTask implements Runnable {

        @Override
        public void run() {
            while (!Thread.currentThread().isInterrupted()) {
                try {
                    long now = System.currentTimeMillis();
                    LoggingEvent event = queue.poll(1, TimeUnit.SECONDS);
                    if (event != null) {
                        byte[] bytes = (event.getRenderedMessage() + "\n").getBytes(StandardCharsets.UTF_8);
                        if (uploadBuffer.writableBytes() < bytes.length || now - lastUploadTimestamp > MAX_UPLOAD_INTERVAL) {
                            upload(now);
                        }
                        uploadBuffer.writeBytes(bytes);
                    } else if (closed) {
                        upload(now);
                        break;
                    } else if (now - lastUploadTimestamp > MAX_UPLOAD_INTERVAL) {
                        upload(now);
                    }
                } catch (InterruptedException e) {
                    break;
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
            }
        }
    }

    private String getObjectKey() {
        String today = LocalDateTime.now(ZoneOffset.UTC).format(DateTimeFormatter.ofPattern("yyyyMMdd"));
        return String.format("automq/logs/cluster/%s/%s/%s", clusterId, today, UUID.randomUUID());
    }
}
