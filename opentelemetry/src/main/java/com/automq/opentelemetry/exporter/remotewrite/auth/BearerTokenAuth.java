package com.automq.opentelemetry.exporter.remotewrite.auth;

import okhttp3.Interceptor;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.Map;

public class BearerTokenAuth implements RemoteWriteAuth {
    private static final Logger LOGGER = LoggerFactory.getLogger(BearerTokenAuth.class);
    private final String token;
    private final Map<String, String> headers;

    public BearerTokenAuth(String token) {
        this(token, Collections.emptyMap());
    }

    public BearerTokenAuth(String token, Map<String, String> headers) {
        this.token = token;
        this.headers = headers;
    }

    public String getToken() {
        return token;
    }

    public Map<String, String> getHeaders() {
        return headers;
    }

    @Override
    public boolean validate() {
        if (StringUtils.isBlank(token)) {
            LOGGER.error("Token is required for bearer token authentication.");
            return false;
        }
        return true;
    }

    @Override
    public AuthType authType() {
        return AuthType.BEARER;
    }

    @Override
    public Interceptor createInterceptor() {
        return new BearerAuthInterceptor(token, headers);
    }
}
