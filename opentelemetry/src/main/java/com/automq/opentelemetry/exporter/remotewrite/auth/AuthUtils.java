package com.automq.opentelemetry.exporter.remotewrite.auth;

import java.util.Arrays;
import java.util.stream.Collectors;

public class AuthUtils {
    public static final String AUTH_HEADER = "Authorization";

    public static String canonicalMIMEHeaderKey(String headerName) {
        return Arrays.stream(headerName.trim().split("-"))
            .map(s -> s.isEmpty() ? s : Character.toUpperCase(s.charAt(0)) + s.substring(1).toLowerCase())
            .collect(Collectors.joining("-"));
    }
}
