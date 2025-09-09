package com.automq.opentelemetry.exporter.remotewrite.auth;

import okhttp3.Interceptor;
import okhttp3.Request;
import okhttp3.Response;

import java.io.IOException;
import java.util.Collections;
import java.util.Map;

public class BearerAuthInterceptor implements Interceptor {
    private final String bearerToken;
    private final Map<String, String> headers;

    public BearerAuthInterceptor(String token) {
        this(token, Collections.emptyMap());
    }

    public BearerAuthInterceptor(String token, Map<String, String> headers) {
        this.bearerToken = token;
        this.headers = headers;
    }

    private boolean validateConfig(String bearerToken) {
        return bearerToken != null && !bearerToken.isEmpty();
    }

    @Override
    public Response intercept(Chain chain) throws IOException {
        Request.Builder builder = chain.request()
            .newBuilder()
            .header("Authorization", "Bearer " + bearerToken);
        headers.forEach(builder::header);
        return chain.proceed(builder.build());
    }
}
