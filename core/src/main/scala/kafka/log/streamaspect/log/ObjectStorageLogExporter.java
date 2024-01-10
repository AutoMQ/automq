package kafka.log.streamaspect.log;

import java.net.URI;
import java.nio.ByteBuffer;
import java.time.format.DateTimeFormatter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.awscore.exception.AwsServiceException;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.HeadObjectRequest;
import software.amazon.awssdk.services.s3.model.HeadObjectResponse;
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
    private ScanOffsetHolder scanOffsetHolder;

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
    public void export(String name, byte[] value) {
        if (currentByteBuffer.remaining() < value.length) {
            flush();
        }
        currentByteBuffer.put(value);
    }

    @Override
    public void flush() {
        try {
            long storeIndex = scanOffsetHolder.getStoreIndex();
            if (currentByteBuffer.position() > 0) {
                String key = getKey(storeIndex);
                if (!scanOffsetHolder.isCleanShutdown()) {
                    HeadObjectResponse response;
                    try {
                        do {
                            HeadObjectRequest headObjectRequest = HeadObjectRequest.builder()
                                .bucket(bucket)
                                .key(key)
                                .build();
                            response = s3Client.headObject(headObjectRequest);
                            key = getKey(++storeIndex);
                        }
                        while (response.eTag() != null);
                    } catch (AwsServiceException e) {
                        LOGGER.info(e.getMessage(), e);
                    }
                }
                PutObjectRequest putObjectRequest = PutObjectRequest.builder()
                    .bucket(bucket)
                    .key(key)
                    .build();
                PutObjectResponse putObjectResponse = s3Client.putObject(putObjectRequest, RequestBody.fromByteBuffer(currentByteBuffer));
                String eTag = putObjectResponse.eTag();
                LOGGER.info("upload file {} to s3, eTag: {}", key, eTag);
                scanOffsetHolder.setStoreIndex(storeIndex);
            }
        } finally {
            currentByteBuffer.clear();
        }
    }

    private String getKey(long storeIndex) {
        return PREFIX + classifier + "/" + String.format("log-%d", storeIndex) + POSTFIX;
    }

    public void setScanOffsetHolder(ScanOffsetHolder scanOffsetHolder) {
        this.scanOffsetHolder = scanOffsetHolder;
    }
}
