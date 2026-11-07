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

package com.automq.shell;

import com.automq.shell.commands.cluster.Cluster;

import picocli.CommandLine;

@CommandLine.Command(name = "automq-cli", mixinStandardHelpOptions = true, version = "automq-cli 1.0",
    description = "Command line tool for maintain AutoMQ cluster(s).",
    subcommands = {
        Cluster.class
    }
)
public class AutoMQCLI {
    public static void main(String... args) {
        int exitCode = new CommandLine(new AutoMQCLI()).execute(args);
        System.exit(exitCode);
    }
}
