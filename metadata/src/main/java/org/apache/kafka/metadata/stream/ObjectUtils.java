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

package org.apache.kafka.metadata.stream;

public class ObjectUtils {
    public static final long NOOP_OBJECT_ID = -1L;
    private static String namespace = "DEFAULT";

    public static void setNamespace(String namespace) {
        ObjectUtils.namespace = namespace;
    }

    public static String genKey(int version, long objectId) {
        if (namespace.isEmpty()) {
            throw new IllegalStateException("NAMESPACE is not set");
        }
        return genKey(version, namespace, objectId);
    }

    public static String genKey(int version, String namespace, long objectId) {
        if (version == 0) {
            String objectIdHex = String.format("%08x", objectId);
            String hashPrefix = new StringBuilder(objectIdHex).reverse().toString();
            return hashPrefix + "/" + namespace + "/" + objectId;
        } else {
            throw new UnsupportedOperationException("Unsupported version: " + version);
        }
    }

}
