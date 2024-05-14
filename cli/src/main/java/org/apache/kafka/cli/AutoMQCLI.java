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

package org.apache.kafka.cli;

import org.apache.kafka.cli.command.Stream;
import picocli.CommandLine;

@CommandLine.Command(name = "automq-cli", mixinStandardHelpOptions = true, version = "automq-cli 1.0",
    description = "Command line tool for maintain AutoMQ cluster(s).",
    subcommands = {
        Stream.class
    })
public class AutoMQCLI {
    @CommandLine.Option(names = {"-b", "--bootstrap-server"}, description = "The Kafka server to connect to.", required = true)
    public String bootstrapServer;

    public static void main(String... args) {
        int exitCode = new CommandLine(new AutoMQCLI()).execute(args);
        System.exit(exitCode);
    }
}