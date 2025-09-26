package com.automq.opentelemetry.exporter.remotewrite.auth;

import okhttp3.Credentials;
import okhttp3.Interceptor;
import okhttp3.Request;
import okhttp3.Response;

import java.io.IOException;
import java.util.Collections;
import java.util.Map;

public class BasicAuthInterceptor implements Interceptor {
    private final String username;
    private final String password;
    private final Map<String, String> headers;

    public BasicAuthInterceptor(String username, String password) {
        this(username, password, Collections.emptyMap());
    }

    public BasicAuthInterceptor(String username, String password, Map<String, String> headers) {
        this.username = username;
        this.password = password;
        this.headers = headers;
    }

    @Override
    public Response intercept(Chain chain) throws IOException {
        Request.Builder builder = chain.request()
            .newBuilder()
            .header(AuthUtils.AUTH_HEADER, Credentials.basic(username, password));
        headers.forEach(builder::header);
        return chain.proceed(builder.build());
    }
}
