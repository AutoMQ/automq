/*
 * Copyright 2026, AutoMQ HK Limited.
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
package com.automq.stream.thirdparty.moe.cnkirito.kdio;

import com.sun.jna.Native;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledOnOs;
import org.junit.jupiter.api.condition.OS;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.LinkedHashSet;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import io.netty.util.internal.PlatformDependent;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

@Tag("S3Unit")
@EnabledOnOs(OS.LINUX)
class DirectIOUtilsTest {
    /** Verifies that releasing a wrapped native allocation actually returns it to the native allocator. */
    @Test
    void releaseFreesNativeAllocation() throws Exception {
        Set<String> classpath = new LinkedHashSet<>();
        for (Class<?> type : new Class<?>[] {ReleaseProbe.class, DirectIOUtils.class, Native.class,
            PlatformDependent.class, LoggerFactory.class}) {
            classpath.add(new File(type.getProtectionDomain().getCodeSource().getLocation().toURI()).getPath());
        }
        ProcessBuilder builder = new ProcessBuilder(
            Path.of(System.getProperty("java.home"), "bin", "java").toString(),
            "--add-opens=java.base/java.nio=ALL-UNNAMED", "-Dio.netty.tryReflectionSetAccessible=true",
            "-Xmx64m", "-cp", String.join(File.pathSeparator, classpath), ReleaseProbe.class.getName());
        // Use libc in a fresh process: free unmaps a large allocation, unlike a caching allocator.
        builder.environment().remove("LD_PRELOAD");
        builder.environment().remove("JAVA_TOOL_OPTIONS");
        builder.environment().remove("JDK_JAVA_OPTIONS");
        builder.environment().put("MALLOC_MMAP_THRESHOLD_", "131072");
        builder.redirectErrorStream(true);
        Process process = builder.start();
        try {
            assertTrue(process.waitFor(30, TimeUnit.SECONDS), "Native release probe timed out");
            String output = new String(process.getInputStream().readAllBytes(), StandardCharsets.UTF_8);
            assertEquals(0, process.exitValue(), output);
        } finally {
            process.destroyForcibly();
        }
    }

    /** Isolates native allocator selection and any failed release from the test JVM. */
    public static class ReleaseProbe {
        /** Checks the real posix_memalign allocation, rather than a mocked release call. */
        public static void main(String[] args) throws Exception {
            ByteBuffer buffer = DirectIOUtils.allocateForDirectIO(new DirectIOLib(4096), 64 << 20);
            long address = PlatformDependent.directBufferAddress(buffer);
            if (!mapped(address)) {
                throw new AssertionError("Native allocation is not mapped");
            }
            DirectIOUtils.releaseDirectBuffer(buffer);
            if (mapped(address)) {
                throw new AssertionError("Native allocation is still mapped after release");
            }
        }

        private static boolean mapped(long address) throws Exception {
            for (String line : Files.readAllLines(Path.of("/proc/self/maps"))) {
                String[] range = line.substring(0, line.indexOf(' ')).split("-");
                long start = Long.parseUnsignedLong(range[0], 16);
                long end = Long.parseUnsignedLong(range[1], 16);
                if (Long.compareUnsigned(address, start) >= 0 && Long.compareUnsigned(address, end) < 0) {
                    return true;
                }
            }
            return false;
        }
    }
}
