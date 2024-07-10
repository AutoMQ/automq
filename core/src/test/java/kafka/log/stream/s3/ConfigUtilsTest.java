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

package kafka.log.stream.s3;

import java.util.Optional;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

class ConfigUtilsTest {

    @Test
    void toWALConfig() {
        ConfigUtils.WALConfig walConfig = ConfigUtils.toWALConfig("/path/to/wal");
        assertEquals("file", walConfig.schema());
        assertEquals("/path/to/wal", walConfig.target());
        assertEquals(Optional.empty(), walConfig.parameter("size"));

        walConfig = ConfigUtils.toWALConfig("0@file:///path/to/wal");
        assertEquals("file", walConfig.schema());
        assertEquals("/path/to/wal", walConfig.target());
        assertEquals(Optional.empty(), walConfig.parameter("size"));

        walConfig = ConfigUtils.toWALConfig("0@file:///path/to/wal?type=raw&size=1G");
        assertEquals("file", walConfig.schema());
        assertEquals("/path/to/wal", walConfig.target());
        assertEquals(Optional.of("raw"), walConfig.parameter("type"));
        assertEquals(Optional.of("1G"), walConfig.parameter("size"));

        walConfig = ConfigUtils.toWALConfig("0@s3://bucket?region=cn-hangzhou");
        assertEquals("s3", walConfig.schema());
        assertEquals("bucket", walConfig.target());
        assertEquals(Optional.of("cn-hangzhou"), walConfig.parameter("region"));
        assertEquals(Optional.empty(), walConfig.parameter("size"));
    }
}
