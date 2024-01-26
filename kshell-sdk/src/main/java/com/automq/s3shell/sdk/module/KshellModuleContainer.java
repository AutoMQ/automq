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
package com.automq.s3shell.sdk.module;

import com.automq.s3shell.sdk.client.S3ClientManager;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.ServiceLoader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class KshellModuleContainer {
    private static final Logger LOGGER = LoggerFactory.getLogger(KshellModuleContainer.class);
    private final Map<String, KshellModule> moduleMap = new HashMap<>();
    private final String instanceId;
    private final S3ClientManager s3ClientManager;
    private final String[] args;
    private Properties configProperties;
    private volatile boolean closed = false;

    public KshellModuleContainer(String instanceId, S3ClientManager s3ClientManager, String[] args) {
        this.instanceId = instanceId;
        this.s3ClientManager = s3ClientManager;
        this.args = args;
        ServiceLoader<KshellModule> serviceLoader = ServiceLoader.load(KshellModule.class);
        for (KshellModule module : serviceLoader) {
            moduleMap.put(module.getModuleName(), module);
        }
    }

    public void start() {
        List<KshellModule> modules = new ArrayList<>(moduleMap.values());
        modules.sort(Comparator.comparingInt(KshellModule::getOrder));
        for (KshellModule module : modules) {
            module.start(this);
        }
    }

    public void close() {
        closed = true;
        for (KshellModule module : moduleMap.values()) {
            try {
                module.close();
            } catch (IOException e) {
                LOGGER.error("Close module failed: " + module.getModuleName(), e);
            }
        }
    }

    public String getInstanceId() {
        return instanceId;
    }

    public S3ClientManager getS3ClientManager() {
        return s3ClientManager;
    }

    public String[] getArgs() {
        return args;
    }

    public Properties getConfigProperties() {
        while (configProperties == null && !closed && !Thread.currentThread().isInterrupted()) {
            try {
                Thread.sleep(5000);
            } catch (InterruptedException e) {
                break;
            }
        }
        return configProperties;
    }

    public void setConfigProperties(Properties configProperties) {
        this.configProperties = configProperties;
    }
}
