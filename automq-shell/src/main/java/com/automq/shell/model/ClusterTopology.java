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

package com.automq.shell.model;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import java.util.Collections;
import java.util.List;

@JsonIgnoreProperties(ignoreUnknown = true)
public class ClusterTopology {
    private Global global;
    private List<Node> controllers;
    private List<Node> brokers;

    public Global getGlobal() {
        return global;
    }

    public void setGlobal(Global global) {
        this.global = global;
    }

    public List<Node> getControllers() {
        return controllers != null ? controllers : Collections.emptyList();
    }

    public void setControllers(List<Node> controllers) {
        this.controllers = controllers;
    }

    public List<Node> getBrokers() {
        return brokers != null ? brokers : Collections.emptyList();
    }

    public void setBrokers(List<Node> brokers) {
        this.brokers = brokers;
    }
}
