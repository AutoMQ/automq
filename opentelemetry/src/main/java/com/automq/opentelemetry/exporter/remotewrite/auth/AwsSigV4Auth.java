package com.automq.opentelemetry.exporter.remotewrite.auth;

import okhttp3.Interceptor;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.Map;

public class AwsSigV4Auth implements RemoteWriteAuth {
    private static final Logger LOGGER = LoggerFactory.getLogger(AwsSigV4Auth.class);
    private final String region;
    private final String accessKey;
    private final String secretKey;
    private final Map<String, String> headers;

    public AwsSigV4Auth(String region, String accessKey, String secretKey) {
        this(region, accessKey, secretKey, Collections.emptyMap());
    }

    public AwsSigV4Auth(String region, String accessKey, String secretKey, Map<String, String> headers) {
        this.region = region;
        this.accessKey = accessKey;
        this.secretKey = secretKey;
        this.headers = headers;
    }

    public String getRegion() {
        return region;
    }

    public String getAccessKey() {
        return accessKey;
    }

    public String getSecretKey() {
        return secretKey;
    }

    @Override
    public boolean validate() {
        if (StringUtils.isBlank(region)) {
            LOGGER.error("Region is required for AWS Sig V4 authentication.");
            return false;
        }
        return true;
    }

    @Override
    public AuthType authType() {
        return AuthType.SIG_V4;
    }

    @Override
    public Interceptor createInterceptor() {
        return new AwsSigV4Interceptor(region, accessKey, secretKey, headers);
    }
}
