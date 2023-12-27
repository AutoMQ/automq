package kafka.log.streamaspect.log;

import java.io.File;
import java.io.RandomAccessFile;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class LogScanner {
    private static final Logger LOGGER = LoggerFactory.getLogger(LogScanner.class);
    private static final int DEFAULT_MAX_SCAN_SIZE = 16 * 1024 * 1024;
    private final List<LogWatchStatus> watchStatuses = new CopyOnWriteArrayList<>();
    private long maxScanSize = DEFAULT_MAX_SCAN_SIZE;
    private final LogExporter logExporter;
    private final ScanOffsetHolder scanOffsetHolder;

    public LogScanner(LogExporter logExporter, ScanOffsetHolder scanOffsetHolder) {
        this.logExporter = logExporter;
        this.scanOffsetHolder = scanOffsetHolder;
    }

    public void addWatch(File file) {
        watchStatuses.add(new LogWatchStatus(file, scanOffsetHolder));
    }

    public void scan() {
        for (LogWatchStatus logWatchStatus : watchStatuses) {
            doScan(logWatchStatus);
        }
        logExporter.flush();
    }

    private void doScan(LogWatchStatus logWatchStatus) {
        File file = logWatchStatus.getFile();
        if (!file.exists()) {
            logWatchStatus.close();
            return;
        }
        long offset = logWatchStatus.getOffset();
        if (file.length() > offset) {
            RandomAccessFile randomAccessFile = logWatchStatus.getRandomAccessFile();
            if (randomAccessFile == null) {
                randomAccessFile = logWatchStatus.reopen();
            }
            if (randomAccessFile == null) {
                LOGGER.warn("file open failed: {}", file.getAbsolutePath());
                return;
            }
            int writed = 0;
            try {
                randomAccessFile.seek(offset);
                String line;
                while ((line = randomAccessFile.readLine()) != null && writed < maxScanSize) {
                    writed += line.length() + 1;
                    logExporter.export(file.getName(), line);
                }
                offset += writed;
                logWatchStatus.saveOffset(offset);
                LOGGER.debug("Export {} bytes to exporter, current offset: {}", writed, offset);
            } catch (Exception e) {
                LOGGER.error(String.format("read file error: %s", file.getAbsolutePath()), e);
                logWatchStatus.reopen();
            }
        } else {
            //TODO 找到更好的办法判断文件被重命名
            logWatchStatus.reopen();
        }
    }

    public void setMaxScanSize(long maxScanSize) {
        this.maxScanSize = maxScanSize;
    }

    public void close() {
        scanOffsetHolder.flush();
        for (LogWatchStatus logWatchStatus : watchStatuses) {
            logWatchStatus.close();
        }
        logExporter.close();
    }
}
