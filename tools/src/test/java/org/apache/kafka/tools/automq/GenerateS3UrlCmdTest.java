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

import java.util.Map;
import net.sourceforge.argparse4j.inf.Namespace;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

class GenerateS3UrlCmdTest {

    @Test
    void run() {
        Namespace namespace = new Namespace(
            Map.of(
                "s3-access-key", "myak",

                "s3-secret-key", "mysk",

                "s3-auth-method", "key-from-args",

                "s3-region", "cn-northwest-1",

                "s3-endpoint-protocol", "https",

                "s3-endpoint", "s3.amazonaws.com",

                "s3-data-bucket", "automq-data-bucket",

                "s3-ops-bucket", "automq-ops-bucket"
            )
        );
        GenerateS3UrlCmd.Parameter parameter = new GenerateS3UrlCmd.Parameter(namespace);
        GenerateS3UrlCmd cmd = new GenerateS3UrlCmd(parameter);
        Assertions.assertTrue(cmd.run().startsWith("s3://s3.amazonaws.com?s3-access-key=myak&s3-secret-key=mysk&s3-region=cn-northwest-1&s3-auth-method=key-from-args&s3-endpoint-protocol=https&s3-data-bucket=automq-data-bucket&s3-ops-bucket=automq-ops-bucket&cluster-id="));
    }
}