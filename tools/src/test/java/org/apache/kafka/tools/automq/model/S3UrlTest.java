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
package org.apache.kafka.tools.automq.model;

import com.automq.shell.model.S3Url;
import java.util.Arrays;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

class S3UrlTest {

    @Test
    void parseTest() {
        String s3Url = "s3://s3.cn-northwest-1.amazonaws.com.cn?s3-access-key=xxx&s3-secret-key=yyy&s3-region=cn-northwest-1&s3-endpoint-protocol=https&s3-data-bucket=wanshao-test&s3-ops-bucket=automq-ops-bucket&cluster-id=fZGPJht6Rf-o7WgrUakLxQ";
        S3Url s3UrlObj = S3Url.parse(s3Url);
        assertEquals("xxx", s3UrlObj.getS3AccessKey());
        assertEquals("yyy", s3UrlObj.getS3SecretKey());
        assertEquals("cn-northwest-1", s3UrlObj.getS3Region());
        assertEquals("https", s3UrlObj.getEndpointProtocol().getName());
        assertEquals("s3.cn-northwest-1.amazonaws.com.cn", s3UrlObj.getS3Endpoint());
        assertEquals("wanshao-test", s3UrlObj.getS3DataBucket());
        assertEquals("automq-ops-bucket", s3UrlObj.getS3OpsBucket());
        assertEquals("fZGPJht6Rf-o7WgrUakLxQ", s3UrlObj.getClusterId());

    }

    @Test
    void parseS3UrlValFromArgs() {
        String s3Url = "s3://s3.cn-northwest-1.amazonaws.com.cn?s3-access-key=xxx&s3-secret-key=yyy&s3-region=cn-northwest-1&s3-endpoint-protocol=https&s3-data-bucket=wanshao-test&s3-ops-bucket=automq-ops-bucket&cluster-id=fZGPJht6Rf-o7WgrUakLxQ";
        String[] args = Arrays.asList("--s3-url=" + s3Url, "--controller-list=\"192.168.123.234:9093\"").toArray(new String[0]);
        String s3urlVal = S3Url.parseS3UrlValFromArgs(args);
        Assertions.assertEquals(s3Url, s3urlVal);
    }
}