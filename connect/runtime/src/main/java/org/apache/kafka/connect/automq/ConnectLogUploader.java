package org.apache.kafka.connect.automq;

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
        com.automq.log.uploader.S3RollingFileAppender.triggerInitialization();
        getLogger().info("Initialized Connect S3 log uploader context");
    }
}
