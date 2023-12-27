package kafka.log.streamaspect.log;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.RandomAccessFile;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class LogWatchStatus {
    private static final Logger LOGGER = LoggerFactory.getLogger(LogWatchStatus.class);
    private final File file;
    private final ScanOffsetHolder offsetHolder;
    private volatile RandomAccessFile randomAccessFile;

    public LogWatchStatus(File file, ScanOffsetHolder offsetHolder) {
        this.file = file;
        this.offsetHolder = offsetHolder;
        reopen();
    }

    public RandomAccessFile reopen() {
        RandomAccessFile origin = this.randomAccessFile;
        RandomAccessFile randomAccessFile = null;
        try {
            randomAccessFile = new RandomAccessFile(file, "r");
        } catch (FileNotFoundException e) {
            LOGGER.warn(String.format("file not found: %s", file.getAbsolutePath()), e);
        } catch (Exception e) {
            LOGGER.error(String.format("open file error: %s", file.getAbsolutePath()), e);
        }
        if (randomAccessFile != null) {
            this.randomAccessFile = randomAccessFile;
            closeInternal(origin);
        }
        return randomAccessFile;
    }

    public RandomAccessFile getRandomAccessFile() {
        return randomAccessFile;
    }

    public File getFile() {
        return file;
    }

    public void saveOffset(long offset) {
        offsetHolder.saveOffset(file.getName(), offset);
    }

    public long getOffset() {
        return offsetHolder.getOffset(file.getName());
    }

    public void close() {
        closeInternal(randomAccessFile);
    }

    private void closeInternal(RandomAccessFile randomAccessFile) {
        if (randomAccessFile != null) {
            try {
                randomAccessFile.close();
            } catch (Exception e) {
                LOGGER.error(e.getMessage(), e);
            }
        }
    }
}
