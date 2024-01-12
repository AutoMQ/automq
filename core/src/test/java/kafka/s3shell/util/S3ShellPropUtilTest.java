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
package kafka.s3shell.util;

import java.io.IOException;
import java.util.Properties;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class S3ShellPropUtilTest {
    @Test
    public void testAutoGenPropsWithBrokerRole() {
        String[] arg = new String[] {
            "--s3-url",
            "s3://s3.cn-northwest-1.amazonaws.com.cn?s3-access-key=xx&s3-secret-key=xx&s3-region=cn-northwest-1&s3-endpoint-protocol=https&s3-data-bucket=wanshao-test&s3-path-style=false&cluster-id=1dxiawPlSI2OlUO6U22-Ew",
            "--override",
            "process.roles=controller",
            "--override",
            "controller.quorum.voters=1@localhost:9093",
            "--override",
            "listeners=CONTROLLER://localhost:9093",
            "--override",
            "node.id=1",
            "--override",
            "autobalancer.reporter.network.out.capacity=12345"
        };
        try {
            Properties prop = S3ShellPropUtil.autoGenPropsByCmd(arg, "controller");
            Assertions.assertEquals("controller", prop.getProperty("process.roles"));
            Assertions.assertEquals("1", prop.getProperty("node.id"));
            Assertions.assertEquals("12345", prop.getProperty("autobalancer.reporter.network.out.capacity"));
        } catch (IOException e) {
            e.printStackTrace();
        }

    }

}
