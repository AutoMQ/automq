package kafka.automq.table;

import com.automq.enterprise.kafka.auth.AliyunInstanceProfileCredentialsProvider;
import com.automq.enterprise.kafka.auth.HuaweiCloudInstanceProfileCredentialsProvider;
import com.automq.enterprise.kafka.auth.TencentCloudInstanceProfileCredentialsProvider;
import com.automq.stream.s3.operator.BucketURI;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import org.apache.commons.lang3.StringUtils;
import software.amazon.awssdk.auth.credentials.AnonymousCredentialsProvider;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.AwsCredentials;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProviderChain;
import software.amazon.awssdk.auth.credentials.InstanceProfileCredentialsProvider;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;

import static com.automq.stream.s3.operator.AwsObjectStorage.AUTH_TYPE_KEY;
import static com.automq.stream.s3.operator.AwsObjectStorage.INSTANCE_AUTH_TYPE;
import static com.automq.stream.s3.operator.AwsObjectStorage.STATIC_AUTH_TYPE;

public class CredentialProviderHolder implements AwsCredentialsProvider {
    private static AwsCredentialsProvider provider;

    public static void setup(AwsCredentialsProvider provider) {
        CredentialProviderHolder.provider = provider;
    }

    public static void setup(BucketURI bucketURI) {
        CredentialProviderHolder.provider = newCredentialsProviderChain(credentialsProviders(bucketURI));
    }

    private static List<AwsCredentialsProvider> credentialsProviders(BucketURI bucketURI) {
        String authType = bucketURI.extensionString(AUTH_TYPE_KEY, STATIC_AUTH_TYPE);
        switch (authType) {
            case STATIC_AUTH_TYPE: {
                String accessKey = bucketURI.extensionString(BucketURI.ACCESS_KEY_KEY, System.getenv("KAFKA_S3_ACCESS_KEY"));
                String secretKey = bucketURI.extensionString(BucketURI.SECRET_KEY_KEY, System.getenv("KAFKA_S3_SECRET_KEY"));
                if (StringUtils.isBlank(accessKey) || StringUtils.isBlank(secretKey)) {
                    return Collections.emptyList();
                }
                return List.of(StaticCredentialsProvider.create(AwsBasicCredentials.create(accessKey, secretKey)));
            }
            case INSTANCE_AUTH_TYPE: {
                AwsCredentialsProvider instanceProfileCredentialsProvider;
                switch (bucketURI.protocol()) {
                    case "s3":
                        instanceProfileCredentialsProvider = InstanceProfileCredentialsProvider.builder().build();
                        break;
                    case "oss":
                        instanceProfileCredentialsProvider = AliyunInstanceProfileCredentialsProvider.builder().build();
                        break;
                    case "obs":
                        instanceProfileCredentialsProvider = HuaweiCloudInstanceProfileCredentialsProvider.builder().build();
                        break;
                    case "cos":
                        instanceProfileCredentialsProvider = TencentCloudInstanceProfileCredentialsProvider.builder().build();
                        break;
                    default:
                        throw new IllegalArgumentException("Unsupported protocol: " + bucketURI.protocol());
                }
                return List.of(instanceProfileCredentialsProvider);
            }
            default:
                throw new UnsupportedOperationException("Unsupported auth type: " + authType);
        }
    }

    private static AwsCredentialsProvider newCredentialsProviderChain(
        List<AwsCredentialsProvider> credentialsProviders) {
        List<AwsCredentialsProvider> providers = new ArrayList<>(credentialsProviders);
        // Add default providers to the end of the chain
        providers.add(InstanceProfileCredentialsProvider.create());
        providers.add(AnonymousCredentialsProvider.create());
        return AwsCredentialsProviderChain.builder()
            .reuseLastProviderEnabled(true)
            .credentialsProviders(providers)
            .build();
    }

    // iceberg will invoke create with reflection.
    public static AwsCredentialsProvider create() {
        return provider;
    }

    @Override
    public AwsCredentials resolveCredentials() {
        throw new UnsupportedOperationException();
    }
}
