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
package org.apache.kafka.common.protocol;

import org.junit.Test;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class ProtoUtilsTest {
    @Test
    public void testDelayedAllocationSchemaDetection() throws Exception {
        //verifies that schemas known to retain a reference to the underlying byte buffer are correctly detected.
        for (ApiKeys key : ApiKeys.values()) {
            switch (key) {
                case PRODUCE:
                case JOIN_GROUP:
                case SYNC_GROUP:
                case SASL_AUTHENTICATE:
                case EXPIRE_DELEGATION_TOKEN:
                case RENEW_DELEGATION_TOKEN:
                case ALTER_USER_SCRAM_CREDENTIALS:
                    assertTrue(key + " should require delayed allocation", key.requiresDelayedAllocation);
                    break;
                default:
                    assertFalse(key + " should not require delayed allocation", key.requiresDelayedAllocation);
                    break;
            }
        }
    }
}
