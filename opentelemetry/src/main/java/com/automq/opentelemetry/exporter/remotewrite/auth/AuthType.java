package com.automq.opentelemetry.exporter.remotewrite.auth;

import java.util.Collection;
import java.util.Map;

public enum AuthType {
    BASIC("basic"),
    SIG_V4("sigv4"),
    BEARER("bearer"),
    AZURE_AD("azuread");

    private static final Map<String, AuthType> NAME_TO_AUTH_TYPE = Map.of(
        "basic", BASIC,
        "sigv4", SIG_V4,
        "bearer", BEARER,
        "azuread", AZURE_AD
    );

    private final String name;

    AuthType(String name) {
        this.name = name;
    }

    public static Collection<String> getNames() {
        return NAME_TO_AUTH_TYPE.keySet();
    }

    public String getName() {
        return name;
    }

    public static AuthType fromName(String name) {
        return NAME_TO_AUTH_TYPE.get(name);
    }
}
