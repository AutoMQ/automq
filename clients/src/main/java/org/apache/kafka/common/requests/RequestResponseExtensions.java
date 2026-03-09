/*
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

package org.apache.kafka.common.requests;

import org.apache.kafka.common.protocol.ApiKeys;

import java.nio.ByteBuffer;
import java.util.ServiceLoader;

public class RequestResponseExtensions {
    private static final RequestResponseExtensionProvider PROVIDER;

    static {
        RequestResponseExtensionProvider loaded = null;
        for (RequestResponseExtensionProvider provider : ServiceLoader.load(RequestResponseExtensionProvider.class)) {
            if (loaded != null) {
                throw new IllegalStateException("Only one RequestResponseExtensionProvider is supported.");
            }
            loaded = provider;
        }
        PROVIDER = loaded;
    }

    public static AbstractRequest parseRequest(ApiKeys apiKey, short apiVersion, ByteBuffer buffer) {
        if (PROVIDER == null) {
            throw new AssertionError(String.format(
                "ApiKey %s is not handled and no extension provider is registered.", apiKey));
        }
        return PROVIDER.parseRequest(apiKey, apiVersion, buffer);
    }

    public static AbstractResponse parseResponse(ApiKeys apiKey, short version, ByteBuffer buffer) {
        if (PROVIDER == null) {
            throw new AssertionError(String.format(
                "ApiKey %s is not handled and no extension provider is registered.", apiKey));
        }
        return PROVIDER.parseResponse(apiKey, version, buffer);
    }
}
