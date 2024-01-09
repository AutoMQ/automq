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
package org.apache.kafka.tools.automq;

import net.sourceforge.argparse4j.ArgumentParsers;
import net.sourceforge.argparse4j.inf.ArgumentParser;

import static net.sourceforge.argparse4j.impl.Arguments.store;

public class AutoMQAdminCmd {

    public static final String ADMIN_TOOL_NAME = "automq-kafka-admin.sh";

    public static final String GENERATE_S3_URL_CMD = "generate-s3-url";
    public static final String GENERATE_LOCAL_START_COMMAND_CMD = "generate-local-start-command";
    public static final String GENERATE_CONFIG_PROPERTIES_CMD = "generate-config-properties";

    static ArgumentParser argumentParser() {
        ArgumentParser parser = ArgumentParsers
            .newArgumentParser("automq-admin-tool")
            .usage(String.format("%s [-h] %s | %s | %s",
                ADMIN_TOOL_NAME,
                GENERATE_S3_URL_CMD,
                GENERATE_LOCAL_START_COMMAND_CMD,
                GENERATE_CONFIG_PROPERTIES_CMD))
            .defaultHelp(true)
            .description("This AutoMQ admin tool contains several tools to help user init and manage AutoMQ cluster easily.");
        parser.addArgument(GENERATE_S3_URL_CMD)
            .action(store())
            .required(false)
            .type(String.class)
            .metavar(GENERATE_S3_URL_CMD)
            .help(String.format("This cmd is used to generate s3url for AutoMQ that is used to connect to s3 or other cloud object storage service. Execute '%s -h' to check its usage.", GENERATE_S3_URL_CMD));
        parser.addArgument("generate-local-start-command")
            .action(store())
            .required(false)
            .type(String.class)
            .metavar("generate-local-start-command")
            .help(String.format("This cmd is used to generate config file and local start command. Execute '%s -h' to check its usage.", GENERATE_LOCAL_START_COMMAND_CMD));
        parser.addArgument("generate-config-properties")
            .action(store())
            .required(false)
            .type(String.class)
            .metavar("generate-config-properties")
            .help(String.format("This cmd is used to generate multi config properties depend on your arguments. Execute '%s -h' to check its usage.", GENERATE_CONFIG_PROPERTIES_CMD));
        return parser;
    }
}
