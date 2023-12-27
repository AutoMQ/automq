package kafka.log.streamaspect.log;

import java.io.File;
import java.io.RandomAccessFile;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CopyOnWriteArrayList;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class LogScanner {
    private static final Logger LOGGER = LoggerFactory.getLogger(LogScanner.class);
    private static final int DEFAULT_MAX_SCAN_SIZE = 64 * 1024;
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
        Map<String, Long> result = new HashMap<>();
        boolean remaining;
        do {
            remaining = false;
            for (LogWatchStatus logWatchStatus : watchStatuses) {
                long offset = doScan(logWatchStatus);
                result.put(logWatchStatus.getFile().getName(), offset);
                if (logWatchStatus.getFile().length() > offset){
                    remaining = true;
                }
            }
        }
        while (remaining);
        try {
            logExporter.flush();
            for (LogWatchStatus logWatchStatus : watchStatuses) {
                Long offset = result.get(logWatchStatus.getFile().getName());
                if (offset != null) {
                    logWatchStatus.saveOffset(offset);
                }
            }
        } catch (Exception e) {
            LOGGER.error(e.getMessage(), e);
        }
    }

    private long doScan(LogWatchStatus logWatchStatus) {
        long offset = logWatchStatus.getOffset();
        File file = logWatchStatus.getFile();
        if (file.length() > offset) {
            RandomAccessFile randomAccessFile = logWatchStatus.getRandomAccessFile();
            if (randomAccessFile == null) {
                randomAccessFile = logWatchStatus.reopen();
            }
            if (randomAccessFile == null) {
                LOGGER.warn("file open failed: {}", file.getAbsolutePath());
                return offset;
            }
            int writed = 0;
            try {
                randomAccessFile.seek(offset);
                String line;
                while ((line = randomAccessFile.readLine()) != null) {
                    if (writed < maxScanSize) {
                        logExporter.export(file.getName(), line);
                        writed += line.length() + 1;
                    }
                }
                offset += writed;
                LOGGER.debug("Export {} bytes to exporter, current offset: {}", writed, offset);
            } catch (Exception e) {
                LOGGER.error(String.format("export file error: %s", file.getAbsolutePath()), e);
                logWatchStatus.reopen();
            }
        } else {
            //TODO 找到更好的办法判断文件被重命名
            logWatchStatus.reopen();
        }
        return offset;
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
