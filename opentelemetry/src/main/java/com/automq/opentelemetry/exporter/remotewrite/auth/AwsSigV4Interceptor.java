package com.automq.opentelemetry.exporter.remotewrite.auth;

import okhttp3.Interceptor;
import okhttp3.Request;
import okhttp3.Response;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Collections;
import java.util.Map;

public class AwsSigV4Interceptor implements Interceptor {
    private static final Logger LOGGER = LoggerFactory.getLogger(AwsSigV4Interceptor.class);
    private final AwsSigV4Signer signer;
    private final Map<String, String> headers;

    public AwsSigV4Interceptor(String region, String accessKey, String secretKey) {
        this(region, accessKey, secretKey, Collections.emptyMap());
    }

    public AwsSigV4Interceptor(String region, String accessKey, String secretKey, Map<String, String> headers) {
        this.signer = new AwsSigV4Signer(region, accessKey, secretKey);
        this.headers = headers;
    }

    @Override
    public Response intercept(Chain chain) throws IOException {
        Request.Builder builder = chain.request().newBuilder();
        headers.forEach(builder::header);
        Request signedRequest = this.signer.sign(builder.build());
        if (signedRequest == null) {
            LOGGER.error("Failed to sign request with AWS Sig V4. Proceeding without signature.");
            return chain.proceed(chain.request());
        }
        return chain.proceed(signedRequest);
    }

}
