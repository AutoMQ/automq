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

import com.automq.shell.model.S3Url;
import com.automq.shell.util.S3PropUtil;
import java.io.File;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import net.sourceforge.argparse4j.inf.Namespace;
import org.apache.commons.io.FileUtils;
import org.apache.kafka.tools.automq.model.ServerGroupConfig;
import org.apache.kafka.tools.automq.util.ConfigParserUtil;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import static com.automq.shell.util.S3PropUtil.BROKER_PROPS_PATH;
import static com.automq.shell.util.S3PropUtil.CONTROLLER_PROPS_PATH;
import static com.automq.shell.util.S3PropUtil.SERVER_PROPS_PATH;

class GenerateConfigFileCmdTest {

    private static GenerateConfigFileCmd cmd;

    @BeforeAll
    public static void setup() {

        Namespace namespace = new Namespace(
            Map.of(
                "s3-url", "myak",

                "broker-list", "mysk",

                "controller-list", "192.168.0.1:9093",

                "network-baseline-bandwidth-mb", 100,

                "controller-only-mode", false

            )
        );
        GenerateConfigFileCmd.Parameter parameter = new GenerateConfigFileCmd.Parameter(namespace);
        cmd = new GenerateConfigFileCmd(parameter);
    }

    @Test
    void loadTemplatePropsTest() {

        Properties props = null;
        try {
            props = S3PropUtil.loadTemplateProps(CONTROLLER_PROPS_PATH);
        } catch (IOException e) {
            e.printStackTrace();
        }
        Assertions.assertNotNull(props);
        Assertions.assertEquals(props.getProperty("controller.quorum.voters"), "1@localhost:9093");

        try {
            props = S3PropUtil.loadTemplateProps(BROKER_PROPS_PATH);
        } catch (IOException e) {
            e.printStackTrace();
        }
        Assertions.assertNotNull(props);
        Assertions.assertEquals(props.getProperty("process.roles"), "broker");

        try {
            props = S3PropUtil.loadTemplateProps(SERVER_PROPS_PATH);
        } catch (IOException e) {
            e.printStackTrace();
        }
        Assertions.assertNotNull(props);
        Assertions.assertEquals(props.getProperty("process.roles"), "broker,controller");
    }

    @Test
    void flushPropsTest() {
        Properties props = null;
        try {
            props = S3PropUtil.loadTemplateProps(BROKER_PROPS_PATH);
        } catch (IOException e) {
            e.printStackTrace();
        }
        Assertions.assertNotNull(props);
        try {
            cmd.flushProps(props, "broker.properties");
        } catch (IOException e) {
            e.printStackTrace();
        }
        Assertions.assertNotNull(props);

        File propFile = new File("generated/broker.properties");
        Properties propsFromFile;
        try {
            propsFromFile = new Properties();
            propsFromFile.load(FileUtils.openInputStream(propFile));
            Assertions.assertEquals("broker", propsFromFile.getProperty("process.roles"));
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        Assertions.assertNotNull(propsFromFile);

    }

    @Test
    void processGroupConfigTest() {

        String s3UrlStr = "s3://s3.cn-northwest-1.amazonaws.com.cn?s3-access-key=xxx&s3-secret-key=yyy&s3-region=cn-northwest-1&s3-endpoint-protocol=https&s3-data-bucket=wanshao-test&cluster-id=HvxKzNetT1GOCUkqCG5eyQ";
        S3Url s3Url = S3Url.parse(s3UrlStr);
        ServerGroupConfig controllerGroupConfig = ConfigParserUtil.genControllerConfig("192.168.0.1:9093;192.168.0.2:9093;192.168.0.3:9093", false);

        ServerGroupConfig brokerGroupConfig = ConfigParserUtil.genBrokerConfig("192.168.0.4:9092;192.168.0.5:9092", controllerGroupConfig);
        try {
            GenerateConfigFileCmd cmd = new GenerateConfigFileCmd(null);
            List<String> propFiles = cmd.processGroupConfig(brokerGroupConfig, BROKER_PROPS_PATH, "broker", s3Url);
            Assertions.assertEquals(2, propFiles.size());
            Properties groupProps = S3PropUtil.loadTemplateProps(propFiles.get(0));
            Assertions.assertEquals(3, groupProps.getProperty("node.id"));

        } catch (IOException e) {
            e.printStackTrace();
        }

    }
}