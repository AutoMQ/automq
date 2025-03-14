/*
 * Copyright 2024, AutoMQ HK Limited.
 *
 * The use of this file is governed by the Business Source License,
 * as detailed in the file "/LICENSE.S3Stream" included in this repository.
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */

package kafka.automq.interceptor;

import com.automq.stream.utils.URIUtils;

import java.net.InetAddress;
import java.util.List;
import java.util.Map;

public class ClientIdMetadata {
    private final String clientId;
    private final Map<String, List<String>> metadata;
    private final InetAddress clientAddress;
    private final String connectionId;

    private ClientIdMetadata(String clientId, InetAddress clientAddress, String connectionId) {
        this.clientId = clientId;
        this.metadata = URIUtils.splitQuery(clientId);
        this.clientAddress = clientAddress;
        this.connectionId = connectionId;
    }

    public static ClientIdMetadata of(String clientId) {
        return new ClientIdMetadata(clientId, null, null);
    }

    public static ClientIdMetadata of(String clientId, InetAddress clientAddress, String connectionId) {
        return new ClientIdMetadata(clientId, clientAddress, connectionId);
    }

    public String rack() {
        List<String> list = metadata.get(ClientIdKey.AVAILABILITY_ZONE);
        if (list == null || list.isEmpty()) {
            return null;
        }
        return list.get(0);
    }

    public ClientType clientType() {
        List<String> list = metadata.get(ClientIdKey.CLIENT_TYPE);
        if (list == null || list.isEmpty()) {
            return null;
        }
        return ClientType.parse(list.get(0));
    }

    public String clientId() {
        return clientId;
    }

    public String connectionId() {
        return connectionId;
    }

    @Override
    public String toString() {
        if (clientAddress == null) {
            return clientId;
        } else {
            return clientId + "/" + clientAddress.getHostAddress();
        }
    }
}
