/*
 * Copyright 2024, AutoMQ CO.,LTD.
 *
 * Use of this software is governed by the Business Source License
 * included in the file BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */

package org.apache.kafka.server.config;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.stream.Collectors;

import static org.apache.kafka.common.utils.ConfigUtils.getBoolean;
import static org.apache.kafka.common.utils.ConfigUtils.getDouble;

public class BrokerQuotaManagerConfig extends ClientQuotaManagerConfig {
    public static final String BROKER_QUOTA_ENABLED_PROP = "broker.quota.enabled";
    public static final String BROKER_QUOTA_PRODUCE_BYTES_PROP = "broker.quota.produce.bytes";
    public static final String BROKER_QUOTA_FETCH_BYTES_PROP = "broker.quota.fetch.bytes";
    public static final String BROKER_QUOTA_REQUEST_PERCENTAGE_PROP = "broker.quota.request.percentage";
    public static final String BROKER_QUOTA_WHITE_LIST_USER_PROP = "broker.quota.white.list.user";
    public static final String BROKER_QUOTA_WHITE_LIST_CLIENT_ID_PROP = "broker.quota.white.list.client.id";
    public static final String BROKER_QUOTA_WHITE_LIST_LISTENER_PROP = "broker.quota.white.list.listener";

    private final int nodeId;

    private boolean quotaEnabled = false;
    private double produceQuota = Double.MAX_VALUE;
    private double fetchQuota = Double.MAX_VALUE;
    private double requestQuota = Double.MAX_VALUE;

    private List<String> userWhiteList = List.of();
    private List<String> clientIdWhiteList = List.of();
    private List<String> listenerWhiteList = List.of();

    public BrokerQuotaManagerConfig(int nodeId, int numQuotaSamples, int quotaWindowSizeSeconds) {
        super(numQuotaSamples, quotaWindowSizeSeconds);
        this.nodeId = nodeId;
    }

    public void update(Properties props) {
        Map<String, Object> map = props.entrySet().stream().collect(Collectors.toMap(e -> e.getKey().toString(), Map.Entry::getValue));
        quotaEnabled = getBoolean(map, BROKER_QUOTA_ENABLED_PROP, false);
        produceQuota = getDouble(map, BROKER_QUOTA_PRODUCE_BYTES_PROP, Double.MAX_VALUE);
        fetchQuota = getDouble(map, BROKER_QUOTA_FETCH_BYTES_PROP, Double.MAX_VALUE);
        requestQuota = getDouble(map, BROKER_QUOTA_REQUEST_PERCENTAGE_PROP, Double.MAX_VALUE);

        String userWhiteListProp = props.getProperty(BROKER_QUOTA_WHITE_LIST_USER_PROP);
        if (null != userWhiteListProp && !userWhiteListProp.isBlank()) {
            userWhiteList = Arrays.asList(userWhiteListProp.split(","));
        }

        String clientIdWhiteListProp = props.getProperty(BROKER_QUOTA_WHITE_LIST_CLIENT_ID_PROP);
        if (null != clientIdWhiteListProp && !clientIdWhiteListProp.isBlank()) {
            clientIdWhiteList = Arrays.asList(clientIdWhiteListProp.split(","));
        }

        String listenerWhiteListProp = props.getProperty(BROKER_QUOTA_WHITE_LIST_LISTENER_PROP);
        if (null != listenerWhiteListProp && !listenerWhiteListProp.isBlank()) {
            listenerWhiteList = Arrays.asList(listenerWhiteListProp.split(","));
        }
    }

    public int nodeId() {
        return nodeId;
    }

    public boolean quotaEnabled() {
        return quotaEnabled;
    }

    public double produceQuota() {
        return produceQuota;
    }

    public double fetchQuota() {
        return fetchQuota;
    }

    public double requestQuota() {
        return requestQuota;
    }

    public List<String> userWhiteList() {
        return userWhiteList;
    }

    public List<String> clientIdWhiteList() {
        return clientIdWhiteList;
    }

    public List<String> listenerWhiteList() {
        return listenerWhiteList;
    }
}
