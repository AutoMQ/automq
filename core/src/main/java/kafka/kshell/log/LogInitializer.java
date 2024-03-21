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

package kafka.kshell.log;

import kafka.kshell.log.helper.LogConfig;
import kafka.kshell.log.helper.Trigger;
import kafka.kshell.log.scanner.Scanner;
import kafka.kshell.log.uploader.s3.S3Uploader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * @author ipsum-0320
 */
public class LogInitializer {
    private static final Logger LOGGER = LoggerFactory.getLogger(LogInitializer.class);
    private final ScheduledExecutorService scheduledExecutorService = Executors.newSingleThreadScheduledExecutor();
    private final S3Uploader s3Uploader;
    private final Scanner scanner;
    private static final int SCAN_INTERVAL = 15;
    // 为了防止内存浪费，当成员变量被声明为 final 常量时，其最好声明为 static。
    private boolean isShutdown = false;
    private int startNums = 0;


    public LogInitializer(LogConfig logConfig) {
        s3Uploader = new S3Uploader(
                logConfig.getS3EndPoint(),
                logConfig.getS3Bucket(),
                logConfig.getS3AccessKey(),
                logConfig.getS3SecretKey(),
                logConfig.getS3Region() + logConfig.getNodeId(),
                logConfig.getLogDir());
        scanner = new Scanner(s3Uploader, logConfig.getLogDir());
    }

    public void start() {
        // start() 只能够调用一次，否则会抛出异常。
        if (startNums > 0) {
            throw new IllegalStateException("The log collection process has already started.");
        }
        startNums++;
        LOGGER.info("Start the log collection process.");
        this.scheduledExecutorService.scheduleAtFixedRate(() -> {
            if (Trigger.getScan()) {
                LOGGER.info("The log collection process is already running.");
                return;
            }
            scanner.scan();
            if (this.isShutdown) {
                this.s3Uploader.close();
            }
        }, 0, SCAN_INTERVAL, TimeUnit.SECONDS);
    }

    public void stop() {
        this.scheduledExecutorService.shutdown();
        // 仅仅是给一个标志位，终止 scheduledExecutorService，。
        this.isShutdown = true;
        LOGGER.info("Terminate the log collection process.");
    }
}
