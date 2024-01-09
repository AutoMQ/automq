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
import net.sourceforge.argparse4j.inf.Namespace;

import static net.sourceforge.argparse4j.impl.Arguments.store;

/**
 * Start kafka server by s3url
 */
public class GenerateConfigFileCmd {
    private final Parameter parameter;

    public GenerateConfigFileCmd(GenerateConfigFileCmd.Parameter parameter) {
        this.parameter = parameter;
    }

    static class Parameter {
        final String s3Url;
        final String controllerIpList;

        final String brokerIpList;

        final String cpuCoreCount;

        final String memorySizeGB;

        final String networkBaselineBandwidthMB;

        Parameter(Namespace res) {
            this.s3Url = res.getString("s3-url");
            this.brokerIpList = res.getString("broker-ip-list");
            this.controllerIpList = res.getString("controller-ip-list");
            this.cpuCoreCount = res.getString("cpu-core-count");
            this.memorySizeGB = res.getString("memory-size-gb");
            this.networkBaselineBandwidthMB = res.getString("network-baseline-bandwidth-mb");
        }
    }

    static ArgumentParser argumentParser() {
        ArgumentParser parser = ArgumentParsers
            .newArgumentParser(AutoMQAdminCmd.GENERATE_CONFIG_PROPERTIES_CMD)
            .defaultHelp(true)
            .description("This cmd is used to generate multi config properties depend on your arguments.");
        parser.addArgument(AutoMQAdminCmd.GENERATE_CONFIG_PROPERTIES_CMD)
            .action(store())
            .required(true);
        parser.addArgument("--s3-url")
            .action(store())
            .required(true)
            .type(String.class)
            .dest("s3-url")
            .metavar("S3-URL")
            .help(String.format("AutoMQ use s3 url to access your s3 and create AutoMQ cluster. You can generate s3 url with cmd '/bin/automq-kafka-admin.sh %s'", AutoMQAdminCmd.GENERATE_S3_URL_CMD));
        parser.addArgument("--controller-ip-list")
            .action(store())
            .required(true)
            .type(String.class)
            .dest("controller-ip-list")
            .metavar("CONTROLLER-IP-LIST")
            .help("Your controller ip:port list, split by ':'. Example: 192.168.0.1:9092;192.168.0.2:9092");
        parser.addArgument("--broker-ip-list")
            .action(store())
            .required(true)
            .type(String.class)
            .dest("broker-ip-list")
            .metavar("BROKER-IP-LIST")
            .help("Your broker ip:port list, split by ':'. Example: 192.168.0.1:9092;192.168.0.2:9092");
        parser.addArgument("--cpu-core-count")
            .action(store())
            .required(true)
            .type(String.class)
            .dest("cpu-core-count")
            .metavar("CPU-CORE-COUNT")
            .help("CPU core count of your machine to run broker or controller");
        parser.addArgument("--memory-size-gb")
            .action(store())
            .required(true)
            .setDefault("https")
            .type(String.class)
            .dest("memory-size-gb")
            .metavar("MEMORY-SIZE-GB")
            .help("Memory size of your machine to run broker or controller");
        parser.addArgument("--network-baseline-bandwidth-mb")
            .action(store())
            .required(true)
            .type(String.class)
            .dest("network-baseline-bandwidth-mb")
            .metavar("NETWORK-BASELINE-BANDWIDTH-MB")
            .help("Network baseline bandwidth of your machine to run broker or controller. Usually you can get it from your cloud provider's official instance document. Example: https://docs.aws.amazon.com/AWSEC2/latest/UserGuide/memory-optimized-instances.html");

        return parser;
    }

    public String run() {
        return "";
    }
}
