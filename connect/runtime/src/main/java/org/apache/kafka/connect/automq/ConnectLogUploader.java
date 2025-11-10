package org.apache.kafka.connect.automq;

import com.automq.log.S3RollingFileAppender;
import com.automq.log.uploader.S3LogConfig;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.Properties;

/**
 * Initializes the AutoMQ S3 log uploader for Kafka Connect.
 */
public final class ConnectLogUploader {
    private static Logger getLogger() {
        return LoggerFactory.getLogger(ConnectLogUploader.class);
    }    
    
    private ConnectLogUploader() {
    }

    public static void initialize(Map<String, String> workerProps) {
        Properties props = new Properties();
        if (workerProps != null) {
            workerProps.forEach((k, v) -> {
                if (k != null && v != null) {
                    props.put(k, v);
                }
            });
        }
        ConnectS3LogConfigProvider.initialize(props);
        S3LogConfig s3LogConfig = new ConnectS3LogConfigProvider().get();
        S3RollingFileAppender.setS3Config(s3LogConfig);
        getLogger().info("Initialized Connect S3 log uploader context");
    }
}
