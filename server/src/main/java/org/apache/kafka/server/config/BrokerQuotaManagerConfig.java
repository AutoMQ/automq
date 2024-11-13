/*
 * Copyright 2024, AutoMQ HK Limited.
 *
 * The use of this file is governed by the Business Source License,
 * as detailed in the file "/LICENSE.S3Stream" included in this repository.
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
    private final int nodeId;

    private boolean quotaEnabled = false;
    private double produceQuota = Double.MAX_VALUE;
    private double fetchQuota = Double.MAX_VALUE;
    private double requestRateQuota = Double.MAX_VALUE;

    private List<String> userWhiteList = List.of();
    private List<String> clientIdWhiteList = List.of();
    private List<String> listenerWhiteList = List.of();

    public BrokerQuotaManagerConfig(int nodeId, int numQuotaSamples, int quotaWindowSizeSeconds) {
        super(numQuotaSamples, quotaWindowSizeSeconds);
        this.nodeId = nodeId;
    }

    public void update(Properties props) {
        Map<String, Object> map = props.entrySet().stream().collect(Collectors.toMap(e -> e.getKey().toString(), Map.Entry::getValue));
        quotaEnabled = getBoolean(map, QuotaConfigs.BROKER_QUOTA_ENABLED_CONFIG, quotaEnabled);
        produceQuota = getDouble(map, QuotaConfigs.BROKER_QUOTA_PRODUCE_BYTES_CONFIG, produceQuota);
        fetchQuota = getDouble(map, QuotaConfigs.BROKER_QUOTA_FETCH_BYTES_CONFIG, fetchQuota);
        requestRateQuota = getDouble(map, QuotaConfigs.BROKER_QUOTA_REQUEST_RATE_CONFIG, requestRateQuota);

        String userWhiteListProp = props.getProperty(QuotaConfigs.BROKER_QUOTA_WHITE_LIST_USER_CONFIG);
        if (null != userWhiteListProp && !userWhiteListProp.isBlank()) {
            userWhiteList = Arrays.asList(userWhiteListProp.split(","));
        }

        String clientIdWhiteListProp = props.getProperty(QuotaConfigs.BROKER_QUOTA_WHITE_LIST_CLIENT_ID_CONFIG);
        if (null != clientIdWhiteListProp && !clientIdWhiteListProp.isBlank()) {
            clientIdWhiteList = Arrays.asList(clientIdWhiteListProp.split(","));
        }

        String listenerWhiteListProp = props.getProperty(QuotaConfigs.BROKER_QUOTA_WHITE_LIST_LISTENER_CONFIG);
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

    public void produceQuota(double produceQuota) {
        this.produceQuota = produceQuota;
    }

    public double fetchQuota() {
        return fetchQuota;
    }

    public void fetchQuota(double fetchQuota) {
        this.fetchQuota = fetchQuota;
    }

    public double requestRateQuota() {
        return requestRateQuota;
    }

    public void requestRateQuota(double requestRateQuota) {
        this.requestRateQuota = requestRateQuota;
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
