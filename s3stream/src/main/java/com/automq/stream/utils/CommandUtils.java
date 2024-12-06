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
