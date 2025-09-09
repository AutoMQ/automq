package com.automq.opentelemetry.exporter.remotewrite.auth;

import okhttp3.Interceptor;

public interface RemoteWriteAuth {
    boolean validate();
    AuthType authType();
    Interceptor createInterceptor();
}
