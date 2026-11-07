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

package com.automq.stream.utils;

public class CommandResult {
    private final int code;
    private final String stdout;
    private final String stderr;

    public CommandResult(int code, String stdout, String stderr) {
        this.code = code;
        this.stdout = stdout;
        this.stderr = stderr;
    }

    /**
     * Returns true if the command exited with a zero exit code.
     */
    public boolean success() {
        return code == 0;
    }

    public int code() {
        return code;
    }

    public String stdout() {
        return stdout;
    }

    public String stderr() {
        return stderr;
    }

    @Override
    public String toString() {
        return "CommandResult{" +
            "code=" + code +
            ", stdout='" + stdout + '\'' +
            ", stderr='" + stderr + '\'' +
            '}';
    }
}
