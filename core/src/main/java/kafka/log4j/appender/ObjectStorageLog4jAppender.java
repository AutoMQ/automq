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

import com.aliyun.oss.ClientBuilderConfiguration;
import com.aliyun.oss.OSS;
import com.aliyun.oss.OSSClientBuilder;
import com.aliyun.oss.OSSException;
import com.aliyun.oss.common.comm.SignVersion;
import com.aliyuncs.exceptions.ClientException;
import com.aliyun.oss.common.auth.CredentialsProviderFactory;
import org.apache.log4j.AppenderSkeleton;
import org.apache.log4j.spi.LoggingEvent;
import com.aliyun.oss.common.auth.EnvironmentVariableCredentialsProvider;


import java.io.ByteArrayInputStream;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.UUID;
import java.util.concurrent.locks.ReentrantLock;

/**
 * @author ipsum-0320
 */
public class ObjectStorageLog4jAppender extends AppenderSkeleton {
    // 存储日志使用的是 oss，Kafka 数据跑在 s3 上。
    private int bufferSize;
    private ByteBuffer logBuffer = null;
    private String endPoint;
    private String nodeId;
    private String bucket;
    private String region;
    private int readyNum = 0;
    private OSS ossClient = null;
    private static final ReentrantLock LOCK = new ReentrantLock();
    private boolean upload = false;

    @Override
    protected void append(LoggingEvent event) {
        // 将日志写入 buffer。
        if (upload || logBuffer == null) {
            return;
        }
        if (logBuffer.remaining() < event.getMessage().toString().getBytes(StandardCharsets.UTF_8).length) {
            // buffer 空间不足，将 buffer 中的日志写入 OSS，这里需要异步写入。
            if (LOCK.tryLock()) {
                upload = true;
                new Thread(() -> {
                    upload();
                    LOCK.unlock();
                    upload = false;
                }).start();
            }
        } else {
            logBuffer.put(event.getMessage().toString().getBytes(StandardCharsets.UTF_8));
            // 使用 UTF-8 来解码数据。
        }
    }

    public void upload() {
        logBuffer.flip();
        byte[] logBytes = new byte[logBuffer.remaining()];
        logBuffer.get(logBytes);
        try {
            ossClient.putObject(
                    bucket,
                    String.format(
                            "logs/%s/%s",
                            nodeId,
                            UUID.randomUUID() + "-" + System.currentTimeMillis()),
                    new ByteArrayInputStream(logBytes)
            );
        } catch (OSSException oe) {
            System.out.println("Caught an OSSException, which means request made it to OSS, but was rejected with an error response for some reason.");
            System.out.println("Error Message:" + oe.getErrorMessage());
            System.out.println("Error Code:" + oe.getErrorCode());
            System.out.println("Request ID:" + oe.getRequestId());
            System.out.println("Host ID:" + oe.getHostId());
        }
        logBuffer.clear();
    }

    public void initOssClient() {
        if (this.ossClient != null) {
            return;
        }
        try {
            EnvironmentVariableCredentialsProvider credentialsProvider = CredentialsProviderFactory.newEnvironmentVariableCredentialsProvider();
            ClientBuilderConfiguration clientBuilderConfiguration = new ClientBuilderConfiguration();
            clientBuilderConfiguration.setSignatureVersion(SignVersion.V4);
            this.ossClient = OSSClientBuilder.create()
                    .endpoint(this.endPoint)
                    .credentialsProvider(credentialsProvider)
                    .clientConfiguration(clientBuilderConfiguration)
                    .region(this.region)
                    .build();
        } catch (ClientException e) {
            // 初始化失败，避免使用 LOGGER 进行日志输出。
            System.out.println("Failed to initialize OSS client.");
        }
    }

    @Override
    public void close() {
        // 释放资源。
        if (logBuffer != null) {
            logBuffer.clear();
            logBuffer = null;
        }
        if (ossClient != null) {
            ossClient.shutdown();
            ossClient = null;
        }
    }

    @Override
    public boolean requiresLayout() {
        // 该 Appender 不需要 Layout。
        return false;
    }

    public void setBufferSize(int bufferSize) {
        if (logBuffer != null) {
            return;
        }
        this.bufferSize = bufferSize;
        logBuffer = ByteBuffer.allocateDirect(this.bufferSize);
    }

    public void setEndPoint(String endPoint) {
        this.endPoint = endPoint;
        readyNum++;
        if (readyNum == 4) {
            initOssClient();
        }
    }

    public void setNodeId(String nodeId) {
        this.nodeId = nodeId;
        readyNum++;
        if (readyNum == 4) {
            initOssClient();
        }
    }

    public void setBucket(String bucket) {
        this.bucket = bucket;
        readyNum++;
        if (readyNum == 4) {
            initOssClient();
        }
    }

    public void setRegion(String region) {
        this.region = region;
        readyNum++;
        if (readyNum == 4) {
            initOssClient();
        }
    }
}
