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

package test.plugins;

import java.util.List;
import java.util.Map;

import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.connect.connector.Task;
import org.apache.kafka.connect.sink.SinkConnector;

/**
 * Fake plugin class for testing classloading isolation.
 * See {@link org.apache.kafka.connect.runtime.isolation.TestPlugins}.
 * <p>Unconditionally throw an exception during the default constructor.
 */
public class DefaultConstructorThrowsConnector extends SinkConnector {


    public DefaultConstructorThrowsConnector() {
        throw new RuntimeException("I always throw an exception");
    }

    @Override
    public String version() {
        return null;
    }

    @Override
    public void start(Map<String, String> props) {

    }

    @Override
    public Class<? extends Task> taskClass() {
        return null;
    }

    @Override
    public List<Map<String, String>> taskConfigs(int maxTasks) {
        return null;
    }

    @Override
    public void stop() {

    }

    @Override
    public ConfigDef config() {
        return null;
    }
}
