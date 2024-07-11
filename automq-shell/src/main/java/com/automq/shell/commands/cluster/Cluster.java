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

package com.automq.shell.commands.cluster;

import com.automq.shell.AutoMQCLI;
import picocli.CommandLine;

@CommandLine.Command(name = "cluster", description = "Cluster operation", mixinStandardHelpOptions = true,
    subcommands = {
        Create.class,
        Deploy.class
    }
)
public class Cluster {
    @CommandLine.ParentCommand
    AutoMQCLI cli;
}
