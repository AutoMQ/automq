package kafka.log.streamaspect.log;

import java.net.URI;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;
import software.amazon.awssdk.services.s3.model.PutObjectResponse;

public class ObjectStorageLogExporter implements LogExporter {
    private static final Logger LOGGER = LoggerFactory.getLogger(ObjectStorageLogExporter.class);
    private static final DateTimeFormatter PATTERN = DateTimeFormatter.ofPattern("yyyyMMddHHmmss");
    private static final String PREFIX = "logs/";
    private static final String POSTFIX = ".log";
    public static final int BUFFER_SIZE = 16 * 1024 * 1024;

    private final S3Client s3Client;
    private final String bucket;
    private final String classifier;
    private final ByteBuffer currentByteBuffer;
    private long index = 0;

    public ObjectStorageLogExporter(String endPoint, String bucket, String accessKey,
        String secretKey, String classifier) {
        this.bucket = bucket;
        this.classifier = classifier;
        StaticCredentialsProvider credentialsProvider = StaticCredentialsProvider.create(AwsBasicCredentials.create(accessKey, secretKey));
        s3Client = S3Client.builder()
            .credentialsProvider(credentialsProvider)
            .endpointOverride(URI.create(endPoint))
            .build();
        currentByteBuffer = ByteBuffer.allocateDirect(BUFFER_SIZE);
    }

    @Override
    public void close() {
        flush();
        s3Client.close();
    }

    @Override
    public void export(String name, String value) {
        byte[] bytes = (name + "|" + value + "\n").getBytes(StandardCharsets.UTF_8);
        if (currentByteBuffer.remaining() < bytes.length) {
            flush();
        }
        currentByteBuffer.put(bytes);
    }

    @Override
    public void flush() {
        try {
            if (currentByteBuffer.position() > 0) {
                long now = System.currentTimeMillis();
                LocalDateTime localDateTime = LocalDateTime.ofEpochSecond(now / 5000 * 5, 0, ZoneOffset.UTC);
                String key = PREFIX + classifier + "/" + localDateTime.format(PATTERN) + "-" + index++ + POSTFIX;
                PutObjectRequest putObjectRequest = PutObjectRequest.builder()
                    .bucket(bucket)
                    .key(key)
                    .build();
                PutObjectResponse putObjectResponse = s3Client.putObject(putObjectRequest, RequestBody.fromByteBuffer(currentByteBuffer));
                String eTag = putObjectResponse.eTag();
                LOGGER.info("upload file {} to s3, eTag: {}", key, eTag);
            }
        } finally {
            currentByteBuffer.clear();
        }
    }
}
