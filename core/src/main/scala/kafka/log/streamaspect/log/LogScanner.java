package kafka.log.streamaspect.log;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
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
    private static final int DEFAULT_MAX_LINE_SIZE = 4 * 1024;
    private final List<LogWatchStatus> watchStatuses = new CopyOnWriteArrayList<>();
    private long maxScanSize = DEFAULT_MAX_SCAN_SIZE;
    private long maxLineSize = DEFAULT_MAX_LINE_SIZE;
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
                if (logWatchStatus.getFile().length() > offset) {
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
                ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
                byte[] bytes;
                while (readLine(byteArrayOutputStream, randomAccessFile, maxLineSize) && writed < maxScanSize) {
                    bytes = byteArrayOutputStream.toByteArray();
                    logExporter.export(file.getName(), bytes);
                    byteArrayOutputStream.reset();
                    writed += bytes.length;
                }
                offset += writed;
                LOGGER.debug("Export {} bytes to exporter, current offset: {}", writed, offset);
            } catch (Exception e) {
                LOGGER.error(String.format("export file error: %s", file.getAbsolutePath()), e);
                logWatchStatus.reopen();
            }
        } else if (offset > file.length()) {
            logWatchStatus.reset();
        }
        return offset;
    }

    private boolean readLine(ByteArrayOutputStream byteArrayOutputStream, RandomAccessFile randomAccessFile,
        long maxLength) throws IOException {
        int c = -1;
        boolean eol = false;
        int size = 0;
        while (!eol && size < maxLength) {
            switch (c = randomAccessFile.read()) {
                case -1:
                    eol = true;
                    break;
                case '\n':
                    byteArrayOutputStream.write(c);
                    ++size;
                    eol = true;
                    break;
                case '\r':
                    eol = true;
                    long cur = randomAccessFile.getFilePointer();
                    if ((randomAccessFile.read()) != '\n') {
                        randomAccessFile.seek(cur);
                    }
                    break;
                default:
                    byteArrayOutputStream.write(c);
                    ++size;
                    break;
            }
        }

        return (c != -1) || size > 0;
    }

    public void setMaxScanSize(long maxScanSize) {
        this.maxScanSize = maxScanSize;
    }

    public void setMaxLineSize(long maxLineSize) {
        this.maxLineSize = maxLineSize;
    }

    public void close() {
        scanOffsetHolder.flush(true);
        for (LogWatchStatus logWatchStatus : watchStatuses) {
            logWatchStatus.close();
        }
        logExporter.close();
    }
}
