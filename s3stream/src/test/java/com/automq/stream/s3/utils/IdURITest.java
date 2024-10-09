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
