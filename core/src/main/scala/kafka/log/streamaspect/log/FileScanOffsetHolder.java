package kafka.log.streamaspect.log;

import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class FileScanOffsetHolder implements ScanOffsetHolder {
    private static final Logger LOGGER = LoggerFactory.getLogger(FileScanOffsetHolder.class);
    private static final String TMP_FILE_NAME = "offset.tmp";
    private static final String OFFSET_FILE_NAME = "offset";
    private static final String CLEAN_SHUTDOWN_SYMBOL_FILE = "clean-shutdown";
    public static final String STORE_INDEX = "_storeIndex:";
    private final Map<String, Long> offsetMap = new ConcurrentHashMap<>();
    private long storeIndex;
    private final ScheduledExecutorService scheduledExecutorService = Executors.newSingleThreadScheduledExecutor();
    private final File workDir;

    private final boolean cleanShutdown;

    public FileScanOffsetHolder(String workPath) {
        this.workDir = new File(workPath);
        if (!workDir.exists()) {
            workDir.mkdirs();
        }
        File offsetFile = new File(workDir, OFFSET_FILE_NAME);
        File tempFile = new File(workDir, TMP_FILE_NAME);
        if (!offsetFile.exists()) {
            offsetFile = tempFile;
        }
        if (offsetFile.exists()) {
            String value = null;
            try {
                value = FileUtils.readFileToString(offsetFile, StandardCharsets.UTF_8);
            } catch (IOException e) {
                LOGGER.error(e.getMessage(), e);
            }
            if (value != null) {
                for (String line : value.split("\n")) {
                    String[] split = line.split(":");
                    if (split.length == 2) {
                        String key = split[0];
                        if (STORE_INDEX.equals(key)) {
                            storeIndex = Long.parseLong(split[1]);
                        } else {
                            offsetMap.put(key, Long.parseLong(split[1]));
                        }
                    }
                }
            }
        }
        File file = new File(workDir, CLEAN_SHUTDOWN_SYMBOL_FILE);
        cleanShutdown = file.exists();
        FileUtils.deleteQuietly(file);
        scheduledExecutorService.scheduleAtFixedRate(() -> flush(false), 5, 10, TimeUnit.SECONDS);
    }

    @Override
    public void flush(boolean shutdown) {
        String content = getContent();
        File offsetFile = new File(workDir, OFFSET_FILE_NAME);
        File tempFile = new File(workDir, TMP_FILE_NAME);
        try {
            FileUtils.writeStringToFile(tempFile, content, StandardCharsets.UTF_8);
            FileUtils.moveFile(tempFile, offsetFile);
        } catch (IOException e) {
            LOGGER.error(e.getMessage(), e);
        }
        if (shutdown) {
            try {
                new File(workDir, CLEAN_SHUTDOWN_SYMBOL_FILE).createNewFile();
            } catch (IOException e) {
                LOGGER.error(e.getMessage(), e);
            }
        }
    }

    @Override
    public long getOffset(String name) {
        return offsetMap.getOrDefault(name, 0L);
    }

    @Override
    public long getStoreIndex() {
        return storeIndex;
    }

    @Override
    public void setStoreIndex(long storeIndex) {
        this.storeIndex = storeIndex;
    }

    private String getContent() {
        StringBuilder stringBuilder = new StringBuilder();
        stringBuilder.append(STORE_INDEX).append(storeIndex).append("\n");
        for (Map.Entry<String, Long> entry : offsetMap.entrySet()) {
            stringBuilder.append(entry.getKey()).append(":").append(entry.getValue()).append("\n");
        }
        return stringBuilder.toString();
    }

    @Override
    public void saveOffset(String name, long offset) {
        offsetMap.put(name, offset);
    }

    @Override
    public boolean isCleanShutdown() {
        return cleanShutdown;
    }

}
