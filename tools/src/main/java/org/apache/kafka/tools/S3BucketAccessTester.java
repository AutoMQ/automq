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

package org.apache.kafka.tools;

import net.sourceforge.argparse4j.ArgumentParsers;
import net.sourceforge.argparse4j.inf.ArgumentParser;
import net.sourceforge.argparse4j.inf.ArgumentParserException;
import net.sourceforge.argparse4j.inf.Namespace;
import net.sourceforge.argparse4j.internal.HelpScreenException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static net.sourceforge.argparse4j.impl.Arguments.store;


/**
 * S3BucketAccessTester is a tool for testing access to S3 buckets.
 */
public class S3BucketAccessTester {
    private static final Logger LOGGER = LoggerFactory.getLogger(S3BucketAccessTester.class);

    static class TestConfig {
        final String endpoint;
        final String region;
        final String bucket;

        TestConfig(Namespace res) {
            this.endpoint = res.getString("endpoint");
            this.region = res.getString("region");
            this.bucket = res.getString("bucket");
        }

        static ArgumentParser parser() {
            ArgumentParser parser = ArgumentParsers
                    .newArgumentParser("S3BucketAccessTester")
                    .defaultHelp(true)
                    .description("Test access to S3 buckets");
            parser.addArgument("-e", "--endpoint")
                    .action(store())
                    .setDefault("s3.amazonaws.com")
                    .type(String.class)
                    .dest("endpoint")
                    .metavar("ENDPOINT")
                    .help("S3 endpoint");
            parser.addArgument("-r", "--region")
                    .action(store())
                    .required(true)
                    .type(String.class)
                    .dest("region")
                    .metavar("REGION")
                    .help("S3 region");
            parser.addArgument("-b", "--bucket")
                    .action(store())
                    .required(true)
                    .type(String.class)
                    .dest("bucket")
                    .metavar("BUCKET")
                    .help("S3 bucket");
            return parser;
        }
    }

    public static void main(String[] args) throws Exception {
        Namespace namespace = null;
        ArgumentParser parser = TestConfig.parser();
        try {
            namespace = parser.parseArgs(args);
        } catch (HelpScreenException e) {
            System.exit(0);
        } catch (ArgumentParserException e) {
            parser.handleError(e);
            System.exit(1);
        }
        TestConfig config = new TestConfig(namespace);
        LOGGER.info("Testing access to S3 bucket {} at endpoint {} in region {}",
                config.bucket, config.endpoint, config.region);
    }
}
