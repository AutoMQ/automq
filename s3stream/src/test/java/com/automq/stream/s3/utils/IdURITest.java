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

package com.automq.stream.s3.utils;

import com.automq.stream.utils.IdURI;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

@Tag("S3Unit")
public class IdURITest {

    @Test
    public void test() {
        String raw = "0@proto://helloworld.com?k1=v1&k1=v2&k3=v3";
        Assertions.assertEquals(raw, IdURI.parse(raw).encode());

        raw = "0@proto://helloworld.com";
        Assertions.assertEquals(raw, IdURI.parse(raw).encode());
    }

}
