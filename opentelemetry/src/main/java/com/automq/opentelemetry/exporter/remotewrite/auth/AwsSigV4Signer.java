package com.automq.opentelemetry.exporter.remotewrite.auth;

import com.automq.opentelemetry.exporter.remotewrite.PromConsts;
import okhttp3.Request;
import okio.Buffer;
import org.apache.commons.lang3.StringUtils;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.awssdk.auth.credentials.InstanceProfileCredentialsProvider;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.http.SdkHttpMethod;
import software.amazon.awssdk.http.SdkHttpRequest;
import software.amazon.awssdk.http.auth.aws.signer.AwsV4HttpSigner;
import software.amazon.awssdk.http.auth.spi.signer.SignedRequest;

import java.io.IOException;
import java.util.Optional;

public class AwsSigV4Signer {
    private final AwsV4HttpSigner signer;
    private final String region;
    private final AwsCredentialsProvider credentialsProvider;

    public AwsSigV4Signer(String region, String accessKey, String secretKey) {
        if (!validateConfig(region)) {
            throw new IllegalArgumentException("Invalid AWS Sig V4 config");
        }
        this.region = region;
        this.signer = AwsV4HttpSigner.create();
        this.credentialsProvider = credentialsProvider(accessKey, secretKey);
    }

    private AwsCredentialsProvider credentialsProvider(String accessKey, String secretKey) {
        if (!StringUtils.isBlank(accessKey) && !StringUtils.isBlank(secretKey)) {
            return StaticCredentialsProvider.create(AwsBasicCredentials.create(accessKey, secretKey));
        }
        return InstanceProfileCredentialsProvider.builder().build();
    }

    private boolean validateConfig(String region) {
        return region != null && !region.isEmpty();
    }

    public Request sign(Request request) throws IOException {
        SdkHttpRequest tmpRequest = SdkHttpRequest.builder()
            .uri(request.url().uri())
            .method(SdkHttpMethod.fromValue(request.method()))
            .headers(request.headers().toMultimap())
            .build();
        try (Buffer buffer = new Buffer()) {
            SignedRequest signedRequest;
            if (request.body() == null) {
                signedRequest = signer.sign(r -> r
                    .identity(credentialsProvider.resolveCredentials())
                    .request(tmpRequest)
                    .putProperty(AwsV4HttpSigner.PAYLOAD_SIGNING_ENABLED, false)
                    .putProperty(AwsV4HttpSigner.SERVICE_SIGNING_NAME, PromConsts.AWS_PROMETHEUS_SERVICE_NAME)
                    .putProperty(AwsV4HttpSigner.REGION_NAME, region));
            } else {
                request.body().writeTo(buffer);
                signedRequest = signer.sign(r -> r
                    .identity(credentialsProvider.resolveCredentials())
                    .request(tmpRequest)
                    .payload(buffer::inputStream)
                    .putProperty(AwsV4HttpSigner.PAYLOAD_SIGNING_ENABLED, true)
                    .putProperty(AwsV4HttpSigner.SERVICE_SIGNING_NAME, PromConsts.AWS_PROMETHEUS_SERVICE_NAME)
                    .putProperty(AwsV4HttpSigner.REGION_NAME, region));
            }
            Optional<String> signature = signedRequest.request().firstMatchingHeader(AuthUtils.AUTH_HEADER);
            if (signature.isPresent()) {
                Request.Builder builder = request.newBuilder();
                signedRequest.request().headers().forEach((k, v) -> {
                    if (v.isEmpty()) {
                        return;
                    }
                    builder.header(AuthUtils.canonicalMIMEHeaderKey(k), v.get(0));
                });
                return builder.build();
            }
            return null;
        }
    }
}

