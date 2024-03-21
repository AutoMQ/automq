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

package kafka.kshell.log.scanner;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import kafka.kshell.log.helper.OffsetMapValue;
import kafka.kshell.log.uploader.Uploader;
import kafka.kshell.log.helper.Trigger;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Timer;
import java.util.TimerTask;
import java.util.HashMap;

public class Scanner {

    private final Uploader uploader;
    private final String logDir;
    // DONE TODO offsetMap 的持久化。
    private Map<String, OffsetMapValue> offsetMap;
    private final List<File> logFileList = new ArrayList<>();

    // 日志文件的数量，其中 auto-balancer.log 和 kafkaServer-gc.log 没有在 log4j 的配置文件中声明。
    private static final int NUM_OF_LOG_FILE = 11;
    private static final Logger LOGGER = LoggerFactory.getLogger(Scanner.class);
    // 4KB
    private static final int MAX_LINE_SIZE = 2 * 1024;
    // 64KB
    private static final int MAX_SCAN_SIZE = 32 * 1024;
    private static final String OFFSET_MAP_FILE_NAME = "offset-map.json";
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
                String mapJson = Files.readString(offsetMapFile.toPath(), StandardCharsets.UTF_8);
                ObjectMapper objectMapper = new ObjectMapper();
                this.offsetMap = objectMapper.readValue(mapJson, new TypeReference<>() {
                });
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
            // 自旋 20s 等待日志文件创建完成。
            while (true) {
                File[] files = dir.listFiles();
                if (files == null) {
                    LOGGER.error("The log file list is empty.");
                    throw new RuntimeException("The log file list is empty.");
                }
                if (files.length >= NUM_OF_LOG_FILE) {
                    break;
                }
                if (count[0] > 20) {
                    // 如果 20 秒内日志文件没有创建完成，那么就打印该信息。
                    LOGGER.error(String.format("Retry %d: The collected log files that were recorded are missing.", count[0]));
                    break;
                }
            }
            timer.cancel();
            File[] files = dir.listFiles();
            if (files == null) {
                LOGGER.error("The log file list is empty.");
                throw new RuntimeException("The log file list is empty.");
            }
            for (File file : files) {
                if (file.isFile()) {
                    if (file.getName().equals(Uploader.BUFFER_FILE_NAME) || file.getName().equals(OFFSET_MAP_FILE_NAME)) {
                        continue;
                    }
                    offsetMap.put(file.getName(), new OffsetMapValue(0L, ""));
                    logFileList.add(file);
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
            Files.write(Paths.get(offsetMapFile.getPath()), mapJson.getBytes(StandardCharsets.UTF_8));
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
                    // logFile 是动态刷新的，即 File 刷新时，其内容也会更新。
                    readLine(byteArrayOutputStream, raf, MAX_LINE_SIZE);
                    String line = byteArrayOutputStream.toString(StandardCharsets.UTF_8);
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
                while (curSize < MAX_SCAN_SIZE && readLine(byteArrayOutputStream, raf, MAX_LINE_SIZE)) {
                    byte[] bytes = byteArrayOutputStream.toByteArray();
                    uploader.toBuffer(bytes);
                    byteArrayOutputStream.reset();
                    curSize += bytes.length;
                }
                offset += curSize;
                // 判断是否是第一次写入日志，如果是，需要记录第一行的内容作为轮转唯一标识。
                long preOffset = offsetMap.get(logFile.getName()).getOffset();
                if (preOffset == 0) {
                    try (RandomAccessFile rotationRaf = new RandomAccessFile(logFile, "r")) {
                        byteArrayOutputStream.reset();
                        readLine(byteArrayOutputStream, rotationRaf, MAX_LINE_SIZE);
                        String line = byteArrayOutputStream.toString(StandardCharsets.UTF_8);
                        byteArrayOutputStream.reset();
                        offsetMap.get(logFile.getName()).setLine(line);
                    } catch (FileNotFoundException e) {
                        LOGGER.error(String.format("File not found: %s", logFile.getAbsolutePath()), e);
                    } catch (IOException e) {
                        LOGGER.error(String.format("Open file error: %s", logFile.getAbsolutePath()), e);
                    }
                }
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
            if (files == null) {
                LOGGER.error("The log file list is empty.");
                throw new RuntimeException("The log file list is empty.");
            }
            for (File file : files) {
                if (file.isFile()) {
                    if (file.getName().equals(logFile.getName())) {
                        // 当前日志文件不需要处理。
                        continue;
                    }
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
                String line = byteArrayOutputStream.toString(StandardCharsets.UTF_8);
                if (!line.equals(keyLine)) {
                    unRecordedFiles.add(file);
                } else {
                    unRecordedFiles.add(file);
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
        offsetMap.get(logFile.getName()).setLine("");
    }
}
