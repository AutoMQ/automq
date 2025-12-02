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

package kafka.server.streamaspect;


import kafka.server.BrokerServer;

import org.apache.kafka.controller.ClusterControlManager;
import org.apache.kafka.controller.FPCManager;
import org.apache.kafka.controller.QuorumController;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.Method;
import java.util.ServiceLoader;

/**
 * Provider responsible for loading and exposing {@link FPCManager} implementations.
 * <p>
 * The provider guarantees that the {@link ServiceLoader} lookup is executed at most once per
 * ClassLoader and exposes the result to both controller (metadata) and broker (core) code paths.
 */
public final class FingerPrintControlManagerProvider {
    private static final Logger LOG = LoggerFactory.getLogger(FingerPrintControlManagerProvider.class);
    private static final Object INIT_LOCK = new Object();

    private static volatile FPCManager cachedInstance;

    private FingerPrintControlManagerProvider() {
        // utility class
    }

    /**
     * Returns the lazily-loaded {@link FPCManager} implementation, or {@code null} if none is found.
     */
    public static FPCManager get() {
        FPCManager current = cachedInstance;
        if (current == null) {
            synchronized (INIT_LOCK) {
                current = cachedInstance;
                if (current == null) {
                    cachedInstance = current = loadService();
                }
            }
        }
        return current;
    }

    /**
     * Convenience helper used by the controller to both retrieve and initialize the implementation
     * (if it exposes a compatible {@code initialize(QuorumController, ClusterControlManager)} method).
     */
    public static FPCManager getAndInitForController(
        QuorumController controller,
        ClusterControlManager clusterControlManager
    ) {
        FPCManager manager = get();
        if (manager != null) {
            initForController(manager, controller, clusterControlManager);
        }
        return manager;
    }

    private static FPCManager loadService() {
        try {
            ServiceLoader<FPCManager> loader =
                ServiceLoader.load(FPCManager.class, FPCManager.class.getClassLoader());

            FPCManager first = null;
            for (FPCManager impl : loader) {
                if (first != null) {
                    LOG.warn("Multiple FingerPrintControlManagerV1 implementations found. Using {}", first.getClass().getName());
                    break;
                }
                first = impl;
                LOG.info("Loaded FingerPrintControlManagerV1 implementation: {}", first.getClass().getName());
            }
            return first;
        } catch (Throwable t) {
            LOG.error("Failed to load FingerPrintControlManagerV1 implementation", t);
            return null;
        }
    }

    private static void initForController(
        FPCManager manager,
        QuorumController controller,
        ClusterControlManager clusterControlManager
    ) {
        try {
            Method initializeMethod = manager.getClass().getMethod(
                "initForController",
                QuorumController.class,
                ClusterControlManager.class
            );
            initializeMethod.invoke(manager, controller, clusterControlManager);
        } catch (Throwable t) {
            LOG.warn("Failed to initialize FingerPrintControlManagerV1 implementation {}", manager.getClass().getName(), t);
        }
    }

    private static void initForBroker(
        FPCManager manager,
        BrokerServer brokerServer
    ) {
        try {
            Method initializeMethod = manager.getClass().getMethod(
                "initForBroker",
                BrokerServer.class
            );
            initializeMethod.invoke(manager, brokerServer);
        } catch (Throwable t) {
            LOG.warn("Failed to initialize FingerPrintControlManagerV1 implementation {}", manager.getClass().getName(), t);
        }
    }

}
