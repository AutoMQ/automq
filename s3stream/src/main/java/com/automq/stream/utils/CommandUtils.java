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

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.charset.Charset;
import java.util.stream.Collectors;

public class CommandUtils {
    public static CommandResult run(String... cmd) {
        return run(runtime -> runtime.exec(cmd));
    }

    public static CommandResult run(CommandCall call) {
        try {
            Process p = call.call(Runtime.getRuntime());
            try (BufferedReader inputReader = new BufferedReader(new InputStreamReader(p.getInputStream(), Charset.defaultCharset()));
                 BufferedReader errorReader = new BufferedReader(new InputStreamReader(p.getErrorStream(), Charset.defaultCharset()))) {
                String stdout = inputReader.lines().collect(Collectors.joining("\n"));
                String stderr = errorReader.lines().collect(Collectors.joining("\n"));
                int code = p.waitFor();
                return new CommandResult(code, stdout, stderr);
            }
        } catch (IOException | InterruptedException e) {
            return new CommandResult(-1, "", e.getMessage());
        }
    }

    public interface CommandCall {
        Process call(Runtime runtime) throws IOException;
    }

}
