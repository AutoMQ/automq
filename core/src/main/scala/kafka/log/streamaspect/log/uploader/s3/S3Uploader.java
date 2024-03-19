package kafka.log.streamaspect.log.uploader.s3;

import kafka.log.streamaspect.log.uploader.Uploader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;
import software.amazon.awssdk.services.s3.model.PutObjectResponse;
import software.amazon.awssdk.services.s3.model.S3Exception;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.net.URI;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.UUID;

/**
 * @author ipsum-0320
 */
public class S3Uploader implements Uploader {
    private static final Logger LOGGER = LoggerFactory.getLogger(S3Uploader.class);
    private final String bucket;
    private final String classifier;
    private final S3Client s3Client;
    private final ByteBuffer currentByteBuffer;
    private final String logDir;

    public S3Uploader(String endPoint, String bucket, String accessKey,
                                    String secretKey, String classifier, String logDir) {
        this.bucket = bucket;
        this.classifier = classifier;
        this.logDir = logDir;
        StaticCredentialsProvider credentialsProvider = StaticCredentialsProvider.create(AwsBasicCredentials.create(accessKey, secretKey));
        // 创建一个 S3 客户端（s3 提供对象存储服务）
        s3Client = S3Client.builder()
                .credentialsProvider(credentialsProvider)
                .endpointOverride(URI.create(endPoint))
                .build();
        // 分配一个直接缓冲区（16 MB），该内存不由 JVM 控制，这里面存放日志，由 storeIndex 进行区分。
        currentByteBuffer = ByteBuffer.allocateDirect(BUFFER_SIZE);
    }

    @Override
    public void toBuffer(byte[] value) {
        if (currentByteBuffer.remaining() < value.length) {
            String key = UUID.randomUUID() + "-" + System.currentTimeMillis();
            upload(key);
        }
        currentByteBuffer.put(value);
    }

    @Override
    public void upload(String key) {
        // 将堆外内存中的数据上传到 s3 中。
        String uploadKey = wrapKey(key);
        // DONE TODO 无限重试。
        int retry = 0;
        while (retry < MAX_RETRY) {
            try {
                if (currentByteBuffer.position() > 0) {
                    PutObjectRequest putObjectRequest = PutObjectRequest.builder()
                            .bucket(bucket)
                            .key(uploadKey)
                            .build();
                    PutObjectResponse putObjectResponse = s3Client.putObject(putObjectRequest, RequestBody.fromByteBuffer(currentByteBuffer));
                    String eTag = putObjectResponse.eTag();
                    // 将 currentByteBuffer 中的数据上传到 Amazon S3 存储服务中。
                    LOGGER.info("upload file {} to s3, eTag: {}", uploadKey, eTag);
                    break;
                }
            } catch (S3Exception e) {
                retry++;
                LOGGER.error("Retry: {}, upload file {} to s3 failed", retry, uploadKey);
            }
        }
        if (retry >= MAX_RETRY) {
            LOGGER.error("upload file {} to s3 failed", uploadKey);
            // 在抛出错误之前，将 buffer 中的数据刷到磁盘上。
            currentByteBuffer.flip();
            try (FileOutputStream fos = new FileOutputStream(new File(this.logDir, BUFFER_FILE_NAME))) {
                FileChannel channel = fos.getChannel();
                channel.write(currentByteBuffer);
            } catch (IOException e) {
                LOGGER.error("The buffer failed to write to the local file, resulting in loss of log data.", e);
            }
            // 清空 buffer，防止内存泄露。
            currentByteBuffer.clear();
            throw S3Exception.builder()
                    .message("upload file " + uploadKey + " to s3 failed")
                    .statusCode(400)
                    .build();
        } else {
            currentByteBuffer.clear();
        }
    }

    @Override
    public void close() {
        upload(SHUTDOWN_SIGNAL + System.currentTimeMillis());
        s3Client.close();
    }

    @Override
    public String wrapKey(String key) {
        return PREFIX + classifier + "-" + key + POSTFIX;
    }

}
