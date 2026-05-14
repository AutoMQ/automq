/*
 * Copyright 2026, AutoMQ HK Limited.
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

package org.apache.kafka.clients.admin;

import org.apache.kafka.common.annotation.InterfaceStability;

/**
 * Options for {@link Admin#describeAutoBalancerDecisionTrace(DescribeAutoBalancerDecisionTraceOptions)}.
 *
 * The API of this class is evolving, see {@link Admin} for details.
 */
@InterfaceStability.Evolving
public class DescribeAutoBalancerDecisionTraceOptions extends AbstractOptions<DescribeAutoBalancerDecisionTraceOptions> {
    public static final String QUERY_LATEST = "LATEST";
    public static final String QUERY_LIST = "LIST";
    public static final String QUERY_GET = "GET";
    public static final String QUERY_LATEST_EXPLANATION_REPORT = "LATEST_EXPLANATION_REPORT";

    private String queryType = QUERY_LATEST;
    private String traceId = "";
    private int limit = 20;

    public String queryType() {
        return queryType;
    }

    public DescribeAutoBalancerDecisionTraceOptions queryType(String queryType) {
        this.queryType = queryType;
        return this;
    }

    public String traceId() {
        return traceId;
    }

    public DescribeAutoBalancerDecisionTraceOptions traceId(String traceId) {
        this.traceId = traceId;
        return this;
    }

    public int limit() {
        return limit;
    }

    public DescribeAutoBalancerDecisionTraceOptions limit(int limit) {
        this.limit = limit;
        return this;
    }
}
