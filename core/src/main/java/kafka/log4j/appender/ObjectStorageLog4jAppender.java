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
import org.apache.log4j.spi.LoggingEvent;


import java.util.concurrent.LinkedBlockingQueue;

/**
 * @author ipsum-0320
 */
public class ObjectStorageLog4jAppender extends AppenderSkeleton {
    // 存储日志使用的是 oss，Kafka 数据跑在 s3 上。
    // 此时需要注意，由于 ObjectStorageLog4jAppender 只被配置在了 rootLogger 上，因此其是单实例的。
    private int queueSize;
    private LinkedBlockingQueue<String> blockQueue = null;
    private String endPoint;
    private String nodeId;
    private String bucket;
    private String region;
    private int readyNum = 0;
    private volatile boolean upload = false;

    @Override
    protected void append(LoggingEvent event) {
        // 将日志写入 buffer。
        if (upload || blockQueue == null) {
            return;
        }
        blockQueue.offer(event.getMessage().toString());
    }

    public void upload() {

    }

    public void initS3Client() {

    }

    @Override
    public void close() {
        // 释放资源。
        if (blockQueue != null) {
            blockQueue = null;
        }
    }

    @Override
    public boolean requiresLayout() {
        // 该 Appender 不需要 Layout。
        return false;
    }

    public void setQueueSize(int queueSize) {
        this.queueSize = queueSize;
        if (blockQueue != null) {
            blockQueue = new LinkedBlockingQueue<>(this.queueSize);
        }
    }

    public void setEndPoint(String endPoint) {
        this.endPoint = endPoint;
        readyNum++;
        if (readyNum == 4) {
        }
    }

    public void setNodeId(String nodeId) {
        this.nodeId = nodeId;
        readyNum++;
        if (readyNum == 4) {

        }
    }

    public void setBucket(String bucket) {
        this.bucket = bucket;
        readyNum++;
        if (readyNum == 4) {
        }
    }

    public void setRegion(String region) {
        this.region = region;
        readyNum++;
        if (readyNum == 4) {
        }
    }
}
