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

package com.automq.shell;

import picocli.CommandLine;

import java.util.concurrent.Callable;

@CommandLine.Command(name = "automq-cli", mixinStandardHelpOptions = true, version = "automq-cli 1.0",
        description = "Command line tool for maintain AutoMQ cluster(s).")
public class AutoMQCLI implements Callable<Integer> {
    @CommandLine.Option(names = {"-b", "--bootstrap-server"}, description = "The Kafka server to connect to.", required = true)
    public String bootstrapServer;
    @CommandLine.Option(names = {"-c", "--command-config"}, description = "Property file containing configs to be passed to Admin Client.")
    public String commandConfig;

    public static void main(String... args) {
        int exitCode = new CommandLine(new AutoMQCLI()).execute(args);
        System.exit(exitCode);
    }

    @Override
    public Integer call() throws Exception {
        String warning = CommandLine.Help.Ansi.AUTO.string("@|bold,yellow Please use the subcommand.|@");
        System.out.println(warning);
        CommandLine.usage(this, System.out);
        return 0;
    }
}