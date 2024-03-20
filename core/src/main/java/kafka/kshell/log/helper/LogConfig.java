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

package kafka.kshell.log.helper;

import com.automq.s3shell.sdk.auth.EnvVariableCredentialsProvider;
import java.util.Properties;

/**
 * @author ipsum-0320
 */
public class LogConfig {
    private final String logDir = System.getenv("LOG_DIR");
    // 需要注意的是，在单机测试的环境下，LOG_DIR 的配置值需要与 JVM 中的参数保持一致。
    private String s3EndPoint;
    private String s3Bucket;
    private final String s3AccessKey = System.getenv(EnvVariableCredentialsProvider.ACCESS_KEY_NAME);
    private final String s3SecretKey = System.getenv(EnvVariableCredentialsProvider.SECRET_KEY_NAME);
    private String s3Region;
    private String nodeId;

    public LogConfig(Properties properties) {
        this.s3EndPoint = properties.getProperty("s3.endpoint");
        this.s3Bucket = properties.getProperty("s3.bucket");
        this.s3Region = properties.getProperty("s3.region");
        this.nodeId = properties.getProperty("node.id");
    }

    public LogConfig(String s3EndPoint, String s3Bucket, String s3Region, String nodeId) {
        this.s3EndPoint = s3EndPoint;
        this.s3Bucket = s3Bucket;
        this.s3Region = s3Region;
        this.nodeId = nodeId;
    }

    public String getLogDir() {
        return logDir;
    }

    public String getS3EndPoint() {
        return s3EndPoint;
    }

    public String getS3Bucket() {
        return s3Bucket;
    }

    public String getS3AccessKey() {
        return s3AccessKey;
    }

    public String getS3SecretKey() {
        return s3SecretKey;
    }

    public String getS3Region() {
        return s3Region;
    }

    public String getNodeId() {
        return nodeId;
    }

    public void setS3EndPoint(String s3EndPoint) {
        this.s3EndPoint = s3EndPoint;
    }

    public void setS3Bucket(String s3Bucket) {
        this.s3Bucket = s3Bucket;
    }

    public void setS3Region(String s3Region) {
        this.s3Region = s3Region;
    }

    public void setNodeId(String nodeId) {
        this.nodeId = nodeId;
    }
}
