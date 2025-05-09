/*
 * Copyright 2025, AutoMQ HK Limited.
 *
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

package kafka.automq.zerozone;

import kafka.automq.zerozone.DefaultClientRackProvider.CIDRMatcher;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

@Timeout(60)
@Tag("S3Unit")
public class DefaultClientRackProviderTest {

    @Test
    public void testCIDRFind() {
        CIDRMatcher matcher = new CIDRMatcher("us-east-1a@10.0.0.0/19,10.0.32.0/19<>us-east-1b@10.0.64.0/19<>us-east-1c@10.0.96.0/19");
        assertEquals("us-east-1a", matcher.find("10.0.31.233").zone());
        assertEquals("10.0.0.0/19", matcher.find("10.0.31.233").cidr());

        assertEquals("us-east-1a", matcher.find("10.0.32.233").zone());
        assertEquals("10.0.32.0/19", matcher.find("10.0.32.233").cidr());

        assertEquals("us-east-1b", matcher.find("10.0.65.0").zone());

        assertEquals("us-east-1c", matcher.find("10.0.97.0").zone());

        assertNull(matcher.find("10.0.128.0"));
    }

}
