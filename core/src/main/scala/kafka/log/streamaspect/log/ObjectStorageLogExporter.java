package kafka.log.streamaspect.log;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;
import software.amazon.awssdk.services.s3.model.PutObjectResponse;

public class ObjectStorageLogExporter implements LogExporter {
    private static final Logger LOGGER = LoggerFactory.getLogger(ObjectStorageLogExporter.class);
    private static final String FORMATTER = "yyyyMMddHHmmss";
    private static final String PREFIX = "logs/";
    private static final String POSTFIX = ".log";

    private final S3Client s3Client;
    private final File workDir;
    private final String bucket;
    private final String classifier;
    private final Object mutex = new Object();
    private volatile BufferedWriter currentWriter;
    private File currentFile;

    public ObjectStorageLogExporter(String workPath, String endPoint, String bucket, String accessKey,
        String secretKey, String classifier) {
        this.bucket = bucket;
        this.classifier = classifier;
        StaticCredentialsProvider credentialsProvider = StaticCredentialsProvider.create(AwsBasicCredentials.create(accessKey, secretKey));
        s3Client = S3Client.builder()
            .credentialsProvider(credentialsProvider)
            .endpointOverride(URI.create(endPoint))
            .build();
        workDir = new File(workPath);
        if (!workDir.exists()) {
            workDir.mkdirs();
        }
    }

    @Override
    public void close() {
        flush();
        s3Client.close();
    }

    @Override
    public boolean export(String name, String value) {
        if (currentWriter == null) {
            synchronized (mutex) {
                if (currentWriter == null) {
                    long now = System.currentTimeMillis();
                    LocalDateTime localDateTime = LocalDateTime.ofEpochSecond(now / 5000 * 5, 0, ZoneOffset.UTC);
                    currentFile = new File(workDir, localDateTime.format(DateTimeFormatter.ofPattern(FORMATTER)) + POSTFIX);
                    try {
                        currentWriter = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(currentFile), StandardCharsets.UTF_8));
                    } catch (IOException e) {
                        throw new RuntimeException(e);
                    }
                }
            }
        }
        try {
            currentWriter.write(name + "|" + value);
            currentWriter.newLine();
            return true;
        } catch (IOException e) {
            LOGGER.error(e.getMessage(), e);
        }
        return false;
    }

    @Override
    public void flush() {
        File uploadFile = currentFile;
        BufferedWriter bufferedWriter = currentWriter;
        currentFile = null;
        currentWriter = null;
        if (uploadFile != null) {
            try {
                try {
                    bufferedWriter.close();
                } catch (IOException e) {
                    LOGGER.error(e.getMessage(), e);
                }
                if (uploadFile.length() > 0) {
                    PutObjectRequest putObjectRequest = PutObjectRequest.builder()
                        .bucket(bucket)
                        .key(PREFIX + classifier + "/" + uploadFile.getName())
                        .build();
                    try {
                        PutObjectResponse putObjectResponse = s3Client.putObject(putObjectRequest, uploadFile.toPath());
                        String eTag = putObjectResponse.eTag();
                        LOGGER.info("upload file {} to s3, eTag: {}", uploadFile.getName(), eTag);
                    } catch (Exception e) {
                        LOGGER.error("upload file {} to s3 failure", uploadFile.getName(), e);
                    }
                }
            } finally {
                FileUtils.deleteQuietly(uploadFile);
            }
        }
    }
}
