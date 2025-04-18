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

package com.automq.stream.s3.cache;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

@Tag("S3Unit")
public class LRUCacheTest {
    @Test
    public void testPeek() {
        LRUCache<String, String> cache = new LRUCache<>();
        cache.put("a", "a1");
        cache.put("b", "b1");
        cache.put("c", "c1");

        Assertions.assertEquals("a1", cache.peek().getValue());
        // peek should not change the order
        Assertions.assertEquals("a1", cache.peek().getValue());

        cache.put("a", "a2");
        Assertions.assertEquals("b1", cache.peek().getValue());
    }

}
