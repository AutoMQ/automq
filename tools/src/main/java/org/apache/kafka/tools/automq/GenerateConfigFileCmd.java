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

import com.automq.shell.constant.ServerConfigKey;
import com.automq.shell.model.S3Url;
import com.automq.shell.util.S3PropUtil;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import net.sourceforge.argparse4j.inf.ArgumentParser;
import net.sourceforge.argparse4j.inf.Namespace;
import net.sourceforge.argparse4j.inf.Subparser;
import org.apache.kafka.tools.automq.model.ServerGroupConfig;
import org.apache.kafka.tools.automq.util.ConfigParserUtil;

import static com.automq.shell.util.S3PropUtil.BROKER_PROPS_PATH;
import static com.automq.shell.util.S3PropUtil.CONTROLLER_PROPS_PATH;
import static com.automq.shell.util.S3PropUtil.SERVER_PROPS_PATH;
import static net.sourceforge.argparse4j.impl.Arguments.store;
import static org.apache.kafka.tools.automq.AutoMQKafkaAdminTool.GENERATE_CONFIG_PROPERTIES_CMD;
import static org.apache.kafka.tools.automq.AutoMQKafkaAdminTool.GENERATE_S3_URL_CMD;

/**
 * Start kafka server by s3url
 */
public class GenerateConfigFileCmd {
    private final Parameter parameter;

    public GenerateConfigFileCmd(Parameter parameter) {
        this.parameter = parameter;
    }

    static class Parameter {
        final String s3Url;
        final String controllerAddress;

        final String brokerAddress;

        final String networkBaselineBandwidthMB;

        final boolean controllerOnlyMode;

        Parameter(Namespace res) {
            this.s3Url = res.getString("s3-url");
            this.brokerAddress = res.getString("broker-list");
            this.controllerAddress = res.getString("controller-list");
            this.networkBaselineBandwidthMB = res.getString("network-baseline-bandwidth-mb");
            this.controllerOnlyMode = res.getBoolean("controller-only-mode");
        }
    }

    public static ArgumentParser addArguments(Subparser parser) {
        parser.addArgument(GENERATE_CONFIG_PROPERTIES_CMD)
            .action(store())
            .required(true);
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
            .help("If this is set to true, all controllers is also seen as broker. If you want to run controller only, set this to true. Default is false.");
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
        S3Url s3Url = S3Url.parse(parameter.s3Url);

        List<String> controllerPropFileNameList;
        ServerGroupConfig controllerGroupConfig;
        if (parameter.controllerOnlyMode) {
            controllerGroupConfig = ConfigParserUtil.genControllerConfig(parameter.controllerAddress, parameter.controllerOnlyMode);
            controllerPropFileNameList = processGroupConfig(controllerGroupConfig, CONTROLLER_PROPS_PATH, "controller", s3Url);
        } else {
            controllerGroupConfig = ConfigParserUtil.genControllerConfig(parameter.controllerAddress, parameter.controllerOnlyMode);
            controllerPropFileNameList = processGroupConfig(controllerGroupConfig, SERVER_PROPS_PATH, "server", s3Url);
        }
        List<String> brokerPropsFileNameList;
        ServerGroupConfig brokerGroupConfig = ConfigParserUtil.genBrokerConfig(parameter.brokerAddress, controllerGroupConfig);
        brokerPropsFileNameList = processGroupConfig(brokerGroupConfig, BROKER_PROPS_PATH, "broker", s3Url);

        System.out.println("############  Generated configuration properties");
        System.out.println("Generated controller or server properties under current directory:");
        for (String propFileName : controllerPropFileNameList) {
            System.out.println(propFileName);
        }
        System.out.println();

        System.out.println("Generated broker under current directory:");
        for (String propFileName : brokerPropsFileNameList) {
            System.out.println(propFileName);
        }
        System.out.println();
    }

    public List<String> processGroupConfig(ServerGroupConfig groupConfig, String propFilePath,
        String outputFilePrefix, S3Url s3Url) throws IOException {
        List<String> propFileNameList = new ArrayList<>();
        for (int i = 0; i < groupConfig.getNodeIdList().size(); i++) {
            int nodeId = groupConfig.getNodeIdList().get(i);
            Properties groupProps = S3PropUtil.loadTemplateProps(propFilePath);
            groupProps.put(ServerConfigKey.NODE_ID.getKeyName(), String.valueOf(nodeId));
            groupProps.put(ServerConfigKey.CONTROLLER_QUORUM_VOTERS.getKeyName(), groupConfig.getQuorumVoters());
            groupProps.put(ServerConfigKey.LISTENERS.getKeyName(), groupConfig.getListenerMap().get(nodeId));
            // use same value as listeners by default
            groupProps.put(ServerConfigKey.ADVERTISED_LISTENERS.getKeyName(), groupConfig.getAdvertisedListenerMap().get(nodeId));
            groupProps.put(ServerConfigKey.S3_ENDPOINT.getKeyName(), s3Url.getEndpointProtocol().getName() + "://" + s3Url.getS3Endpoint());
            groupProps.put(ServerConfigKey.S3_REGION.getKeyName(), s3Url.getS3Region());
            groupProps.put(ServerConfigKey.S3_BUCKET.getKeyName(), s3Url.getS3DataBucket());
            groupProps.put(ServerConfigKey.S3_PATH_STYLE.getKeyName(), s3Url.isS3PathStyle());

            String fileName = String.format("%s-%s.properties", outputFilePrefix, nodeId);
            flushProps(groupProps, fileName);
            propFileNameList.add(fileName);
        }
        return propFileNameList;
    }

    protected void flushProps(Properties props, String fileName) throws IOException {
        S3PropUtil.persist(props, fileName);
    }

}
