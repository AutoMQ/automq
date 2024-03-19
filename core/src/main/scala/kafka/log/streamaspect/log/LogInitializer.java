package kafka.log.streamaspect.log;

import kafka.log.stream.s3.ConfigUtils;
import kafka.log.streamaspect.log.helper.Trigger;
import kafka.log.streamaspect.log.scanner.Scanner;
import kafka.log.streamaspect.log.uploader.s3.S3Uploader;
import kafka.server.KafkaConfig;
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
    private final String CLUSTER = "defaultCluster";
    private final S3Uploader s3Uploader;
    private final Scanner scanner;
    private final int SCAN_INTERVAL = 15;
    private boolean isShutdown = false;
    private int startNums = 0;


    public LogInitializer(KafkaConfig kafkaConfig) {
        String accessKey = System.getenv(ConfigUtils.ACCESS_KEY_NAME);
        String secretKey = System.getenv(ConfigUtils.SECRET_KEY_NAME);
        String logDir = System.getenv("LOG_DIR");
        s3Uploader = new S3Uploader(kafkaConfig.s3Endpoint(), kafkaConfig.s3Bucket(),
                accessKey, secretKey, CLUSTER + kafkaConfig.nodeId(), logDir);
        scanner = new Scanner(s3Uploader, logDir);
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
