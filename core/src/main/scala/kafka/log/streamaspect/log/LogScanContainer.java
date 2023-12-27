package kafka.log.streamaspect.log;

import java.io.File;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import kafka.log.stream.s3.ConfigUtils;
import kafka.server.KafkaConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class LogScanContainer {
    private static final Logger LOGGER = LoggerFactory.getLogger(LogScanContainer.class);
    private static final Object SIGNAL = new Object();
    private LogScanner logScanner;
    private final BlockingQueue<Object> trigger = new LinkedBlockingQueue<>(1);

    private final ScheduledExecutorService scheduledExecutorService = Executors.newSingleThreadScheduledExecutor();
    private final Thread mainThread;
    private volatile boolean closed = false;

    public LogScanContainer(KafkaConfig kafkaConfig) {
        String accessKey = System.getenv(ConfigUtils.ACCESS_KEY_NAME);
        String secretKey = System.getenv(ConfigUtils.SECRET_KEY_NAME);
        String logDir = System.getenv("LOG_DIR");
        if (logDir == null) {
            logDir = "/opt/automq/logs";
        }
        //TODO 配置工作目录，专属bucket, 获取clusterId
        LogExporter exporter = new ObjectStorageLogExporter(kafkaConfig.s3Endpoint(), kafkaConfig.s3Bucket(),
            accessKey, secretKey, "defaultCluster-" + kafkaConfig.brokerId());
        logScanner = new LogScanner(exporter, new FileScanOffsetHolder("/tmp/logscan"));
        //TODO 目录监听配置
        logScanner.addWatch(new File(logDir + "/server.log"));
        logScanner.addWatch(new File(logDir + "/s3stream-threads.log"));
        logScanner.addWatch(new File(logDir + "/log-cleaner.log"));
        logScanner.addWatch(new File(logDir + "/controller.log"));
        logScanner.addWatch(new File(logDir + "/state-change.log"));
        scheduledExecutorService.scheduleAtFixedRate(() -> trigger.offer(SIGNAL), 30, 5, TimeUnit.SECONDS);
        mainThread = new Thread(() -> {
            while (!closed && !Thread.currentThread().isInterrupted()) {
                try {
                    trigger.take();
                } catch (InterruptedException e) {
                    break;
                }
                try {
                    logScanner.scan();
                } catch (Exception e) {
                    LOGGER.error(e.getMessage(), e);
                }
            }
        });
        Runtime.getRuntime().addShutdownHook(new Thread(this::destroy));
    }

    public void start() {
        mainThread.start();
    }

    public void destroy() {
        closed = true;
        scheduledExecutorService.shutdown();
        logScanner.close();
        mainThread.interrupt();
    }
}
