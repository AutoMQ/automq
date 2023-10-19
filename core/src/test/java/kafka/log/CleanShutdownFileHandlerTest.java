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

package kafka.log;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.io.File;
import java.nio.file.Files;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;


@Timeout(value = 10)
class CleanShutdownFileHandlerTest {
    @Test
    public void testCleanShutdownFileBasic() {
        File logDir;
        logDir = assertDoesNotThrow(() -> Files.createTempDirectory("kafka-cleanShutdownFile").toFile());
        CleanShutdownFileHandler cleanShutdownFileHandler = new CleanShutdownFileHandler(logDir.getPath());
        assertDoesNotThrow(() -> cleanShutdownFileHandler.write(10L));
        assertTrue(cleanShutdownFileHandler.exists());
        assertEquals(10L, cleanShutdownFileHandler.read());
        assertDoesNotThrow(() -> cleanShutdownFileHandler.delete());
        assertFalse(cleanShutdownFileHandler.exists());
    }

    @Test
    public void testCleanShutdownFileNonExist() {
        File logDir;
        logDir = assertDoesNotThrow(() -> Files.createTempDirectory("kafka-cleanShutdownFile").toFile());
        CleanShutdownFileHandler cleanShutdownFileHandler = new CleanShutdownFileHandler(logDir.getPath());
        assertDoesNotThrow(() -> cleanShutdownFileHandler.write(10L, 0));
        assertTrue(cleanShutdownFileHandler.exists());
        assertDoesNotThrow(() -> cleanShutdownFileHandler.delete());
        assertFalse(cleanShutdownFileHandler.exists());
        assertEquals(-1L, cleanShutdownFileHandler.read());
    }
}