/*
 * Copyright 2024, AutoMQ HK Limited.
 *
 * Use of this software is governed by the Business Source License
 * included in the file BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */

package com.automq.stream.s3.utils;

import com.automq.stream.utils.SecretUtils;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

@Tag("S3Unit")
public class SecretUtilsTest {

    @Test
    public void testMask() {
        Assertions.assertEquals("12****78", SecretUtils.mask("12345678"));
        Assertions.assertEquals("1*****7", SecretUtils.mask("1234567"));
        Assertions.assertEquals("***", SecretUtils.mask("123"));
    }
}
