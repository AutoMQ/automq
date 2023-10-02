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
package org.apache.kafka.tools;

import kafka.utils.TestInfoUtils;
import org.apache.kafka.common.utils.Exit;

import java.io.ByteArrayOutputStream;
import java.io.PrintStream;

public class ToolsTestUtils {
    /** @see TestInfoUtils#TestWithParameterizedQuorumName()  */
    public static final String TEST_WITH_PARAMETERIZED_QUORUM_NAME = "{displayName}.quorum={0}";

    public static String captureStandardOut(Runnable runnable) {
        return captureStandardStream(false, runnable);
    }

    public static String captureStandardErr(Runnable runnable) {
        return captureStandardStream(true, runnable);
    }

    private static String captureStandardStream(boolean isErr, Runnable runnable) {
        ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
        PrintStream currentStream = isErr ? System.err : System.out;
        PrintStream tempStream = new PrintStream(outputStream);
        if (isErr)
            System.setErr(tempStream);
        else
            System.setOut(tempStream);
        try {
            runnable.run();
            return new String(outputStream.toByteArray()).trim();
        } finally {
            if (isErr)
                System.setErr(currentStream);
            else
                System.setOut(currentStream);

            tempStream.close();
        }
    }

    public static class MockExitProcedure implements Exit.Procedure {
        private boolean hasExited = false;
        private int statusCode;

        @Override
        public void execute(int statusCode, String message) {
            if (!this.hasExited) {
                this.hasExited = true;
                this.statusCode = statusCode;
            }
        }

        public boolean hasExited() {
            return hasExited;
        }

        public int statusCode() {
            return statusCode;
        }
    }
}
