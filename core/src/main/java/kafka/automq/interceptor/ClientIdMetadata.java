/*
 * Copyright 2025, AutoMQ HK Limited.
 *
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package kafka.automq.interceptor;

import com.automq.stream.utils.URIUtils;

import java.net.InetAddress;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;

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

    public InetAddress clientAddress() {
        return clientAddress;
    }

    public List<String> metadata(String key) {
        List<String> value = metadata.get(key);
        return Objects.requireNonNullElse(value, Collections.emptyList());
    }

    public void metadata(String key, List<String> valueList) {
        metadata.put(key, valueList);
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
