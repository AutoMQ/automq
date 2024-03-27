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

package kafka.log4j.appender;

import org.apache.log4j.AppenderSkeleton;
import org.apache.log4j.PatternLayout;
import org.apache.log4j.spi.LoggingEvent;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;
import software.amazon.awssdk.services.s3.model.S3Exception;


import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.util.UUID;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

/**
 * @author ipsum-0320
 */
public class ObjectStorageLog4jAppender extends AppenderSkeleton {
    // 存储日志使用的是 oss，Kafka 数据跑在 s3 上。
    // 此时需要注意，由于 ObjectStorageLog4jAppender 只被配置在了 rootLogger 上，因此其是单实例的。
    private int queueSize = -1;
    private String endPoint = null;
    private String nodeId = null;
    private String bucket = null;
    private String pattern = null;
    private String systemAccessKey = null;
    private String systemSecretKey = null;
    private LinkedBlockingQueue<String> blockQueue = null;
    private S3Client s3Client = null;
    private String accessKey = null;
    private String secretKey = null;
    private PatternLayout layout = null;
    private final Thread uploadThread = new Thread(() -> {
        try {
            StringBuilder logContent = new StringBuilder();
            int count = 0;
            while (!Thread.currentThread().isInterrupted()) {
                String log = blockQueue.poll(1000, TimeUnit.MILLISECONDS);
                if (log == null) {
                    continue;
                }
                logContent.append(log);
                count++;
                if (s3Client != null && count >= queueSize) {
                    upload(logContent.toString());
                    logContent.setLength(0);
                    count = 0;
                }
            }
        } catch (InterruptedException e) {
            System.err.printf("Thread interrupted: %s", e.getMessage());
        }
    });

    @Override
    protected void append(LoggingEvent event) {
        // 将日志写入 buffer。
        if (blockQueue == null || layout == null) {
            return;
        }
        blockQueue.offer(layout.format(event));
    }

    public void upload(String logContent) {
        try {
            PutObjectRequest putObjectRequest = PutObjectRequest.builder()
                    .bucket(bucket)
                    .key(String.format("logs/node-%s/%s", nodeId, UUID.randomUUID() + "-" + System.currentTimeMillis()))
                    .build();
            s3Client.putObject(putObjectRequest, RequestBody.fromString(logContent, StandardCharsets.UTF_8));
        } catch (S3Exception e) {
            // 如果有异常，那么上传失败。
            System.err.printf("Upload failed: %s", e.getMessage());
        }
    }

    public void initS3Client() {
        if (s3Client != null) {
            return;
        }
        StaticCredentialsProvider credentialsProvider = StaticCredentialsProvider.create(AwsBasicCredentials.create(accessKey, secretKey));
        s3Client = S3Client.builder()
                .credentialsProvider(credentialsProvider)
                .endpointOverride(URI.create(endPoint))
                .build();
    }

    @Override
    public void close() {
        // 释放资源。
        if (blockQueue != null) {
            blockQueue = null;
        }
        if (s3Client != null) {
            s3Client.close();
            s3Client = null;
        }
        uploadThread.interrupt();
    }

    @Override
    public boolean requiresLayout() {
        // 该 Appender 不需要 Layout。
        return false;
    }

    private boolean shouldInitS3Client() {
        return s3Client == null &&
                endPoint != null &&
                nodeId != null &&
                bucket != null &&
                accessKey != null &&
                secretKey != null;
    }

    private void init() {
        if (blockQueue == null && queueSize > 0) {
            blockQueue = new LinkedBlockingQueue<>(this.queueSize);
            uploadThread.start();
        }
        if (accessKey == null && systemAccessKey != null) {
            this.accessKey = System.getenv(this.systemAccessKey);
        }
        if (secretKey == null && systemSecretKey != null) {
            this.secretKey = System.getenv(this.systemSecretKey);
        }
        if (layout == null && pattern != null) {
            this.layout = new PatternLayout(this.pattern);
        }
        if (shouldInitS3Client()) {
            initS3Client();
        }
    }


    public void setQueueSize(int queueSize) {
        this.queueSize = queueSize;
        init();
    }

    public void setEndPoint(String endPoint) {
        this.endPoint = endPoint;
        init();
    }

    public void setNodeId(String nodeId) {
        this.nodeId = nodeId;
        init();
    }

    public void setBucket(String bucket) {
        this.bucket = bucket;
        init();
    }

    public void setPattern(String pattern) {
        this.pattern = pattern;
        init();

    }

    public void setSystemAccessKey(String systemAccessKey) {
        this.systemAccessKey = systemAccessKey;
        init();
    }

    public void setSystemSecretKey(String systemSecretKey) {
        this.systemSecretKey = systemSecretKey;
        init();
    }
}
