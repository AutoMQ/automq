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

package com.automq.stream.utils;

import java.io.BufferedReader;
import java.io.IOException;

public class CommandUtils {
    public static CommandResult run(String... cmd) {
        try {
            Process p = Runtime.getRuntime().exec(cmd);
            try (BufferedReader inputReader = p.inputReader();
                 BufferedReader errorReader = p.errorReader()) {
                String stdout = String.join("\n", inputReader.lines().toList());
                String stderr = String.join("\n", errorReader.lines().toList());
                int code = p.waitFor();
                return new CommandResult(code, stdout, stderr);
            }
        } catch (IOException | InterruptedException e) {
            return new CommandResult(-1, "", e.getMessage());
        }
    }

}
