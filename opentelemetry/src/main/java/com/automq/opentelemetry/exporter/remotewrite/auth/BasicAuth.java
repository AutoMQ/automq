package com.automq.opentelemetry.exporter.remotewrite.auth;

import okhttp3.Interceptor;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.Map;

public class BasicAuth implements RemoteWriteAuth {
    private static final Logger LOGGER = LoggerFactory.getLogger(BasicAuth.class);
    private final String username;
    private final String password;
    private final Map<String, String> headers;

    public BasicAuth(String username, String password) {
        this(username, password, Collections.emptyMap());
    }

    public BasicAuth(String username, String password, Map<String, String> headers) {
        this.username = username;
        this.password = password;
        this.headers = headers;
    }

    public String getUsername() {
        return username;
    }

    public String getPassword() {
        return password;
    }

    @Override
    public boolean validate() {
        if (StringUtils.isBlank(username) || StringUtils.isBlank(password)) {
            LOGGER.error("Username and password are required for basic authentication.");
            return false;
        }
        return true;
    }

    @Override
    public AuthType authType() {
        return AuthType.BASIC;
    }

    @Override
    public Interceptor createInterceptor() {
        return new BasicAuthInterceptor(username, password, headers);
    }
}
