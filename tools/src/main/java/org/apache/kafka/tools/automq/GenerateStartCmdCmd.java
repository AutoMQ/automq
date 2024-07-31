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

import java.io.IOException;
import java.util.Arrays;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import net.sourceforge.argparse4j.inf.ArgumentParser;
import net.sourceforge.argparse4j.inf.Namespace;
import net.sourceforge.argparse4j.inf.Subparser;
import org.apache.kafka.tools.automq.model.ServerGroupConfig;
import org.apache.kafka.tools.automq.util.ConfigParserUtil;

import static net.sourceforge.argparse4j.impl.Arguments.store;
import static org.apache.kafka.tools.automq.AutoMQKafkaAdminTool.GENERATE_S3_URL_CMD;

public class GenerateStartCmdCmd {
    private final Parameter parameter;

    public GenerateStartCmdCmd(Parameter parameter) {
        this.parameter = parameter;
    }

    static class Parameter {
        final String s3Url;
        final String controllerList;

        final String brokerList;

        final String networkBaselineBandwidthMB;

        final boolean controllerOnlyMode;

        Parameter(Namespace res) {
            this.s3Url = res.getString("s3-url").replace("http://", "").replace("https://", "");
            this.brokerList = res.getString("broker-list");
            this.controllerList = res.getString("controller-list");
            this.networkBaselineBandwidthMB = res.getString("network-baseline-bandwidth-mb");
            this.controllerOnlyMode = res.getBoolean("controller-only-mode");
        }
    }

    public static ArgumentParser addArguments(Subparser parser) {
        parser.addArgument("--s3-url")
            .action(store())
            .required(true)
            .type(String.class)
            .dest("s3-url")
            .metavar("S3-URL")
            .help(String.format("AutoMQ use s3 url to access your s3 and create AutoMQ cluster. You can generate s3 url with cmd 'bin/automq-kafka-admin.sh %s'", GENERATE_S3_URL_CMD));
        parser.addArgument("--controller-list")
            .action(store())
            .required(true)
            .type(String.class)
            .dest("controller-list")
            .metavar("CONTROLLER-LIST")
            .help("Your controller ip:port list, split by ':'. Example: 192.168.0.1:9092;192.168.0.2:9092");
        parser.addArgument("--broker-list")
            .action(store())
            .required(true)
            .type(String.class)
            .dest("broker-list")
            .metavar("BROKER-LIST")
            .help("Your broker ip:port list, split by ':'. Example: 192.168.0.1:9092;192.168.0.2:9092");
        parser.addArgument("--controller-only-mode")
            .action(store())
            .required(false)
            .type(Boolean.class)
            .dest("controller-only-mode")
            .setDefault(false)
            .metavar("CONTROLLER-ONLY-MODE")
            .help("If this is set to false, all controllers is also seen as broker. If you want to run controller only, set this to true. Default is false.");
        parser.addArgument("--network-baseline-bandwidth-mb")
            .action(store())
            .required(false)
            .type(Integer.class)
            .dest("network-baseline-bandwidth-mb")
            .metavar("NETWORK-BASELINE-BANDWIDTH-MB")
            .help("Network baseline bandwidth of your machine to run broker or controller. Usually you can get it from your cloud provider's official instance document. Example: https://docs.aws.amazon.com/AWSEC2/latest/UserGuide/memory-optimized-instances.html");

        return parser;
    }

    public void run() throws IOException {
        ServerGroupConfig controllerGroupConfig = ConfigParserUtil.genControllerConfig(parameter.controllerList, parameter.controllerOnlyMode);
        ServerGroupConfig brokerGroupConfig = ConfigParserUtil.genBrokerConfig(parameter.brokerList, controllerGroupConfig);

        System.out.println("############  Start Commandline ##############");
        System.out.println("To start an AutoMQ Kafka server, please navigate to the directory where your AutoMQ tgz file is located and run the following command. \n");
        System.out.println("Before running the command, make sure that JDK17 is installed on your host. You can verify the Java version by executing 'java -version'.");
        System.out.println();

        for (int controllerNodeId : controllerGroupConfig.getNodeIdList()) {
            if (parameter.controllerOnlyMode) {
                System.out.printf("bin/kafka-server-start.sh "
                    + "--s3-url=\"%s\" "
                    + "--override process.roles=controller "
                    + "--override node.id=%s "
                    + "--override controller.quorum.voters=%s "
                    + "--override listeners=%s %n", parameter.s3Url, controllerNodeId, controllerGroupConfig.getQuorumVoters(), controllerGroupConfig.getListenerMap().get(controllerNodeId));
            } else {
                System.out.printf("bin/kafka-server-start.sh "
                    + "--s3-url=\"%s\" "
                    + "--override process.roles=broker,controller "
                    + "--override node.id=%s "
                    + "--override controller.quorum.voters=%s "
                    + "--override listeners=%s "
                    + "--override advertised.listeners=%s %n", parameter.s3Url, controllerNodeId, controllerGroupConfig.getQuorumVoters(), controllerGroupConfig.getListenerMap().get(controllerNodeId), controllerGroupConfig.getAdvertisedListenerMap().get(controllerNodeId));
            }
            System.out.println();
        }
        Set<String> nodes = convert2Nodes(parameter.brokerList);
        if (!parameter.controllerOnlyMode) {
            Optional.ofNullable(convert2Nodes(parameter.controllerList)).ifPresent(nodes::removeAll);
        }
        if (!nodes.isEmpty()) {
            for (int brokerNodeId : brokerGroupConfig.getNodeIdList()) {
                System.out.printf("bin/kafka-server-start.sh "
                    + "--s3-url=\"%s\" "
                    + "--override process.roles=broker "
                    + "--override node.id=%s "
                    + "--override controller.quorum.voters=%s "
                    + "--override listeners=%s "
                    + "--override advertised.listeners=%s %n", parameter.s3Url, brokerNodeId, brokerGroupConfig.getQuorumVoters(), brokerGroupConfig.getListenerMap().get(brokerNodeId), brokerGroupConfig.getAdvertisedListenerMap().get(brokerNodeId));
                System.out.println();
            }
        }
        System.out.println();
        System.out.println("TIPS: Start controllers first and then the brokers.");
        System.out.println();
    }

    private Set<String> convert2Nodes(String list) {
        return Arrays.stream(list.split(";")).map(broker -> broker.split(":")[0]).collect(Collectors.toSet());
    }

}
