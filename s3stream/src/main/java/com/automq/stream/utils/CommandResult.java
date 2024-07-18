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
