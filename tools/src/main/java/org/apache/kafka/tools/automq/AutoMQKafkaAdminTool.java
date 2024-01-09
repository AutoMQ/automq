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
            case AutoMQAdminCmd.GENERATE_CONFIG_PROPERTIES_CMD:
                processGenConfigPropertiesCmd(args);
                break;
            default:
                System.out.println(String.format("Not supported command %s. Check usage first.", args[0]));
                parser.printHelp();
                Exit.exit(0);
        }

        Exit.exit(0);

    }

    private static void processGenerateS3UrlCmd(String[] args) {

        Namespace res = null;
        ArgumentParser genS3UrlParser = GenerateS3UrlCmd.argumentParser();
        try {
            res = genS3UrlParser.parseArgs(args);
        } catch (ArgumentParserException e) {
            if (args.length == 1) {
                genS3UrlParser.printHelp();
                Exit.exit(0);
            } else {
                genS3UrlParser.handleError(e);
                Exit.exit(1);
            }
        }

        if (res == null) {
            genS3UrlParser.handleError(new ArgumentParserException("GenerateS3UrlCmd's namespace is null", genS3UrlParser));
            Exit.exit(1);
        }

        GenerateS3UrlCmd.Parameter parameter = new GenerateS3UrlCmd.Parameter(res);

        GenerateS3UrlCmd generateS3UrlCmd = new GenerateS3UrlCmd(parameter);
        try {
            generateS3UrlCmd.run();
        } catch (Throwable t) {
            System.out.printf("FAILED: Caught exception %s%n%n", t.getMessage());
            t.printStackTrace();
            Exit.exit(1);
        }
    }

    private static void processGenConfigPropertiesCmd(String[] args) {
        Namespace res = null;
        ArgumentParser parser = GenerateConfigFileCmd.argumentParser();
        try {
            res = parser.parseArgs(args);
        } catch (ArgumentParserException e) {
            if (args.length == 1) {
                parser.printHelp();
                Exit.exit(0);
            } else {
                parser.handleError(e);
                Exit.exit(1);
            }
        }

        if (res == null) {
            parser.handleError(new ArgumentParserException("GenerateConfigFileCmd's namespace is null", parser));
            Exit.exit(1);
        } else {
            try {
                new GenerateConfigFileCmd(new GenerateConfigFileCmd.Parameter(res)).run();
            } catch (Throwable t) {
                System.out.printf("FAILED: Caught exception %s%n%n", t.getMessage());
                t.printStackTrace();
                Exit.exit(1);
            }
        }

    }

}
