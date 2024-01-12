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

import net.sourceforge.argparse4j.inf.ArgumentParser;
import net.sourceforge.argparse4j.inf.ArgumentParserException;
import net.sourceforge.argparse4j.inf.Namespace;
import org.apache.kafka.common.utils.Exit;

public class AutoMQKafkaAdminTool {
    public static void main(String[] args) {
        // suppress slf4j inner warning log
        System.err.close();
        ArgumentParser parser = AutoMQAdminCmd.argumentParser();
        if (args.length == 0) {
            System.out.println("Please pass valid arguments. Check usage first.");
            parser.printHelp();
            Exit.exit(0);
        }
        switch (args[0]) {
            case AutoMQAdminCmd.GENERATE_S3_URL_CMD:
                processGenerateS3UrlCmd(args);
                break;
            case AutoMQAdminCmd.GENERATE_START_COMMAND_CMD:
                processGenerateStartCmd(args);
                break;
            default:
                System.out.println(String.format("Not supported command %s. Check usage first.", args[0]));
                parser.printHelp();
                Exit.exit(0);
        }

        Exit.exit(0);

    }

    private static Namespace parseArguments(ArgumentParser parser, String[] args) {
        try {
            return parser.parseArgs(args);
        } catch (ArgumentParserException e) {
            if (args.length == 1) {
                parser.printHelp();
                Exit.exit(0);
            } else {
                parser.handleError(e);
                Exit.exit(1);
            }
            return null; // 这行代码实际上不会被执行，但 Java 要求有返回值或抛出异常
        }
    }

    private static void runCommandWithParameter(ArgumentParser parser, Namespace res, Command command) {
        if (res == null) {
            parser.handleError(new ArgumentParserException("Namespace is null", parser));
            Exit.exit(1);
        } else {
            try {
                command.run();
            } catch (Exception e) {
                System.out.printf("FAILED: Caught exception %s%n%n", e.getMessage());
                e.printStackTrace();
                Exit.exit(1);
            }
        }
    }

    @FunctionalInterface
    public interface Command {
        void run() throws Exception;
    }

    private static void processGenerateS3UrlCmd(String[] args) {
        ArgumentParser genS3UrlParser = GenerateS3UrlCmd.argumentParser();
        Namespace res = parseArguments(genS3UrlParser, args);
        runCommandWithParameter(genS3UrlParser, res, () -> new GenerateS3UrlCmd(new GenerateS3UrlCmd.Parameter(res)).run());
    }

    private static void processGenerateStartCmd(String[] args) {
        ArgumentParser parser = GenerateStartCmdCmd.argumentParser();
        Namespace res = parseArguments(parser, args);
        runCommandWithParameter(parser, res, () -> new GenerateStartCmdCmd(new GenerateStartCmdCmd.Parameter(res)).run());
    }

    private static void processGenConfigPropertiesCmd(String[] args) {
        ArgumentParser parser = GenerateConfigFileCmd.argumentParser();
        Namespace res = parseArguments(parser, args);
        runCommandWithParameter(parser, res, () -> new GenerateConfigFileCmd(new GenerateConfigFileCmd.Parameter(res)).run());
    }

}
