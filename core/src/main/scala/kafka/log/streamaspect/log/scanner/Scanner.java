package kafka.log.streamaspect.log.scanner;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import kafka.log.streamaspect.log.helper.OffsetMapValue;
import kafka.log.streamaspect.log.helper.Trigger;
import kafka.log.streamaspect.log.uploader.Uploader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.*;

public class Scanner {

    private final Uploader uploader;
    private final String logDir;
    // DONE TODO offsetMap 的持久化。
    private Map<String, OffsetMapValue> offsetMap;
    private final List<File> logFileList = new ArrayList<>();

    // 日志文件的数量，其中 auto-balancer.log 和 kafkaServer-gc.log 没有在 log4j 的配置文件中声明。
    private final int NUM_OF_LOG_FILE = 11;
    private static final Logger LOGGER = LoggerFactory.getLogger(Scanner.class);
    // 4KB
    private final int MAX_LINE_SIZE = 4 * 1024;
    // 64KB
    private final int MAX_SCAN_SIZE = 64 * 1024;
    private final String OFFSET_MAP_FILE_NAME = "offset-map.json";
    private final File offsetMapFile;


    public Scanner(Uploader uploader, String logDir) {
        this.uploader = uploader;
        this.logDir = logDir;
        File dir = new File(this.logDir);
        File bufferFile = new File(this.logDir, Uploader.BUFFER_FILE_NAME);
        if (bufferFile.exists()) {
            // 检验 buffer 文件是否存在。
            doUploadFileBuffer(bufferFile);
            boolean isDeleted = bufferFile.delete();
            LOGGER.info("The buffer file is deleted: " + isDeleted);
        }
        this.offsetMapFile = new File(this.logDir, OFFSET_MAP_FILE_NAME);
        if (!this.offsetMapFile.exists()) {
            // 生成 offsetMap 文件。
            try {
                boolean isCreated = this.offsetMapFile.createNewFile();
                LOGGER.info("The offset map file is created: " + isCreated);
            } catch (IOException e) {
                LOGGER.error("Create offset map file error.", e);
            }
        }
        initOffsetMap(dir);
    }

    public void initOffsetMap(File dir) {
        // 从文件 offset-map.json 中读取 offsetMap。
        if (offsetMapFile.length() > 0) {
            // 检验 offset 文件中是否存在内容。
            try {
                String mapJson = new String(Files.readAllBytes(offsetMapFile.toPath()));
                ObjectMapper objectMapper = new ObjectMapper();
                this.offsetMap = objectMapper.readValue(mapJson, new TypeReference<>() {});
                return;
            } catch (IOException e) {
                LOGGER.error("Read offset map file error.", e);
            }
        }
        // 初始化 offsetMap。
        if (dir.isDirectory()) {
            offsetMap = new HashMap<>();
            Timer timer = new Timer();
            int[] count = {0};
            timer.schedule(new TimerTask() {
                @Override
                public void run() {
                    count[0]++;
                }
            }, 0, 1000);
            // 自旋 10s 等待日志文件创建完成。
            while (dir.listFiles() == null || Objects.requireNonNull(dir.listFiles()).length != NUM_OF_LOG_FILE) {
                if (count[0] > 10) {
                    // 如果 10 秒内日志文件没有创建完成，那么就打印该信息。
                    LOGGER.error("The collected log files that were recorded are missing.");
                    break;
                }
            }
            File[] files = dir.listFiles();
            timer.cancel();
            ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
            for (File file : Objects.requireNonNull(files)) {
                if (file.isFile()) {
                    try (RandomAccessFile raf = new RandomAccessFile(file, "r")) {
                        readLine(byteArrayOutputStream, raf, MAX_LINE_SIZE);
                        String line = byteArrayOutputStream.toString();
                        // 无日志文件返回空字符串。
                        byteArrayOutputStream.reset();
                        offsetMap.put(file.getName(), new OffsetMapValue(0L, line));
                        logFileList.add(file);
                    } catch (FileNotFoundException e) {
                        LOGGER.error(String.format("File not found: %s", file.getAbsolutePath()), e);
                    } catch (IOException e) {
                        LOGGER.error(String.format("Open file error: %s", file.getAbsolutePath()), e);
                    }
                }
            }
        }
    }

    public void doUploadFileBuffer(File bufferFile) {
        try {
            byte[] buffer = Files.readAllBytes(bufferFile.toPath());
            uploader.toBuffer(buffer);
        } catch (IOException e) {
            LOGGER.error("Read buffer file error.", e);
        }
    }

    public void scan() {
        // 扫描日志文件。
        Trigger.setScan(true);
        for (File logFile : logFileList) {
            doScan(logFile, offsetMap.get(logFile.getName()).getOffset());
        }
        // scan 结束后，要将 offset 刷到磁盘上。
        ObjectMapper objectMapper = new ObjectMapper();
        try {
            String mapJson = objectMapper.writeValueAsString(offsetMap);
            Files.write(Paths.get(offsetMapFile.getPath()), mapJson.getBytes());
        } catch (IOException e) {
            LOGGER.error("Write offset map to file error.", e);
        }
        Trigger.setScan(false);
    }

    public void doScan(File logFile, Long offset) {
        // 如果文件的长度大于当前的偏移量，说明文件中还有未处理的数据，进入处理流程。
        // DONE TODO 日志轮转校验。
        if (logFile.length() > offset) {
            try (RandomAccessFile raf = new RandomAccessFile(logFile, "r")) {
                ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
                if (!offsetMap.get(logFile.getName()).getLine().isEmpty()) {
                    readLine(byteArrayOutputStream, raf, MAX_LINE_SIZE);
                    String line = byteArrayOutputStream.toString();
                    byteArrayOutputStream.reset();
                    if (!line.equals(offsetMap.get(logFile.getName()).getLine())) {
                        // 日志发生了轮转。
                        doRotation(logFile, offset);
                        return;
                    }
                }
                // 日志没有发生轮转。
                raf.seek(offset);
                // 目前读取的数据大小。
                int curSize = 0;
                while (readLine(byteArrayOutputStream, raf, MAX_LINE_SIZE) && curSize < MAX_SCAN_SIZE) {
                    byte[] bytes = byteArrayOutputStream.toByteArray();
                    uploader.toBuffer(bytes);
                    byteArrayOutputStream.reset();
                    curSize += bytes.length;
                }
                offset += curSize;
                // 更新 offset。
                offsetMap.get(logFile.getName()).setOffset(offset);
            } catch (FileNotFoundException e) {
                LOGGER.error(String.format("File not found: %s", logFile.getAbsolutePath()), e);
            } catch (IOException e) {
                LOGGER.error(String.format("Open file error: %s", logFile.getAbsolutePath()), e);
            }
        }
    }

    public boolean readLine(ByteArrayOutputStream byteArrayOutputStream, RandomAccessFile randomAccessFile, int maxLineSize) throws IOException {
        // 按照字节读取，这里直接使用 int 来容纳。
        int container = -1;
        boolean eol = false;
        int curSize = 0;
        while (!eol && curSize < maxLineSize) {
            switch (container = randomAccessFile.read()) {
                case -1:
                    // 读到了文件的末尾。
                    eol = true;
                    break;
                case '\n':
                    // 读到了换行符。
                    byteArrayOutputStream.write(container);
                    ++curSize;
                    eol = true;
                    break;
                default:
                    // 正常读取数据。
                    byteArrayOutputStream.write(container);
                    ++curSize;
                    break;
            }
        }
        // 说明这一行不为空，可以尝试读取下一行。
        return (container != -1) || curSize > 0;
    }

    public void doRotation(File logFile, Long offset) {
        // 轮转日志的命名规律为：xxx.log.yyyy-mm-dd-hh。
        String fileName = logFile.getName();
        File dir = new File(this.logDir);
        List<File> rotationLogFiles = new ArrayList<>();
        if (dir.isDirectory()) {
            File[] files = dir.listFiles();
            for (File file : files) {
                if (file.isFile()) {
                    if (file.getName().startsWith(fileName)) {
                        rotationLogFiles.add(file);
                    }
                }
            }
        }
        rotationLogFiles.sort(Comparator.comparing(File::getName).reversed());
        List<File> unRecordedFiles = new ArrayList<>();
        // 接下来找到没有被记录的 .log 文件。
        String keyLine = offsetMap.get(logFile.getName()).getLine();
        ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();

        for (File file : rotationLogFiles) {
            // 倒序寻找，过滤出没有被记录的日志文件。
            try (RandomAccessFile raf = new RandomAccessFile(file, "r")) {
                readLine(byteArrayOutputStream, raf, MAX_LINE_SIZE);
                String line = byteArrayOutputStream.toString();
                if (!line.equals(keyLine)) {
                    unRecordedFiles.add(file);
                } else {
                    break;
                }
                byteArrayOutputStream.reset();
            } catch (FileNotFoundException e) {
                LOGGER.error(String.format("File not found: %s", file.getAbsolutePath()), e);
            } catch (IOException e) {
                LOGGER.error(String.format("Open file error: %s", file.getAbsolutePath()), e);
            }
        }

        unRecordedFiles.sort(Comparator.comparing(File::getName));
        // 参数传入的 offset 是最旧的未记录日志文件的 offset。
        for (File file : unRecordedFiles) {
            // 开始处理未记录的日志文件。
            try (RandomAccessFile raf = new RandomAccessFile(file, "r")) {
                raf.seek(offset);
                while (readLine(byteArrayOutputStream, raf, MAX_LINE_SIZE)) {
                    byte[] bytes = byteArrayOutputStream.toByteArray();
                    uploader.toBuffer(bytes);
                    byteArrayOutputStream.reset();
                }
                offset = 0L;
            } catch (FileNotFoundException e) {
                LOGGER.error(String.format("File not found: %s", file.getAbsolutePath()), e);
            } catch (IOException e) {
                LOGGER.error(String.format("Open file error: %s", file.getAbsolutePath()), e);
            }
        }
        offsetMap.get(logFile.getName()).setOffset(0L);
    }
}
