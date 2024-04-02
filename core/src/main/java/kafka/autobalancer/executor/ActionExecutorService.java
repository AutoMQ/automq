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

package kafka.autobalancer.executor;

import kafka.autobalancer.common.Action;
import org.apache.kafka.common.config.ConfigException;

import java.util.List;
import java.util.Map;

public interface ActionExecutorService {

    void start();

    void shutdown();

    void execute(Action action);

    void execute(List<Action> actions);

    void validateReconfiguration(Map<String, Object> configs) throws ConfigException;

    void reconfigure(Map<String, Object> configs);
}
