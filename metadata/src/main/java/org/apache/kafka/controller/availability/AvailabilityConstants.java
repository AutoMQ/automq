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

package org.apache.kafka.controller.availability;

public final class AvailabilityConstants {
    public static final int SCHEMA_VERSION = 1;
    public static final String SIGNAL_NAMESPACE = "__automq_avl_v1_s";
    public static final String ACTION_NAMESPACE = "__automq_avl_v1_a";
    public static final String RESPONSE_NAMESPACE = "__automq_avl_v1_r";
    public static final String CONTROLLER_ACTION_NAMESPACE = "__automq_avl_v1_c";

    private AvailabilityConstants() {
    }
}
