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

package kafka.automq.zerozone;

import com.automq.stream.s3.metadata.ObjectUtils;

public class ZoneRouterPack {
    public static final byte PRODUCE_DATA_BLOCK_MAGIC = 0x01;
    public static final int FOOTER_SIZE = 48;
    public static final long PACK_MAGIC = 0x88e241b785f4cff9L;

    public static final String ZONE_ROUTER_CLIENT_ID = "__automq_zr";

    public static String genObjectPath(int nodeId, long objectId) {
        return getObjectPathPrefixBuilder(nodeId).append(objectId).toString();
    }

    public static String getObjectPathPrefix(int nodeId) {
        return getObjectPathPrefixBuilder(nodeId).toString();
    }

    private static StringBuilder getObjectPathPrefixBuilder(int nodeId) {
        return new StringBuilder(String.format("%08x", nodeId)).reverse().append("/").append(ObjectUtils.getNamespace()).append("/router/");
    }
}
