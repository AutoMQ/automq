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
    private final Map<String, Long> offsetMap = new ConcurrentHashMap<>();
    private final ScheduledExecutorService scheduledExecutorService = Executors.newSingleThreadScheduledExecutor();
    private final File workDir;

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
                        offsetMap.put(split[0], Long.parseLong(split[1]));
                    }
                }
            }
        }
        scheduledExecutorService.scheduleAtFixedRate(this::flush, 5, 10, TimeUnit.SECONDS);
    }

    @Override
    public void flush() {
        String content = getContent();
        File offsetFile = new File(workDir, OFFSET_FILE_NAME);
        File tempFile = new File(workDir, TMP_FILE_NAME);
        try {
            FileUtils.writeStringToFile(tempFile, content, StandardCharsets.UTF_8);
            FileUtils.moveFile(tempFile, offsetFile);
        } catch (IOException e) {
            LOGGER.error(e.getMessage(), e);
        }
    }

    @Override
    public long getOffset(String name) {
        return offsetMap.getOrDefault(name, 0L);
    }

    private String getContent() {
        StringBuilder stringBuilder = new StringBuilder();
        for (Map.Entry<String, Long> entry : offsetMap.entrySet()) {
            stringBuilder.append(entry.getKey()).append(":").append(entry.getValue()).append("\n");
        }
        return stringBuilder.toString();
    }

    @Override
    public void saveOffset(String name, long offset) {
        offsetMap.put(name, offset);
    }

}
