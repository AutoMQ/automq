/*
 * Copyright 2024, AutoMQ CO.,LTD.
 *
 * Use of this software is governed by the Business Source License
 * included in the file BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */
package com.automq.shell.model;

public enum EndpointProtocol {

    HTTP("http"),
    HTTPS("https");

    EndpointProtocol(String key) {
        this.name = key;
    }

    private final String name;

    public String getName() {
        return name;
    }

    public static EndpointProtocol getByName(String protocolName) {
        for (EndpointProtocol protocol : EndpointProtocol.values()) {
            if (protocol.getName().equals(protocolName)) {
                return protocol;
            }
        }
        throw new IllegalArgumentException("Invalid protocol: " + protocolName);
    }
}
