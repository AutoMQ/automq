/*
 * Copyright 2025, AutoMQ HK Limited.
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

package kafka.server.streamaspect;



import org.apache.kafka.controller.LicenseManager;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ServiceLoader;

public final class LicenseManagerProvider {
    private static final Logger LOG = LoggerFactory.getLogger(LicenseManagerProvider.class);
    private static final Object INIT_LOCK = new Object();

    private static volatile LicenseManager cachedInstance;

    private LicenseManagerProvider() {
    }

    public static LicenseManager get() {
        LicenseManager current = cachedInstance;
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

    private static LicenseManager loadService() {
        try {
            ServiceLoader<LicenseManager> loader =
                ServiceLoader.load(LicenseManager.class, LicenseManager.class.getClassLoader());
            LicenseManager first = null;
            for (LicenseManager impl : loader) {
                if (first != null) {
                    break;
                }
                first = impl;
            }
            return first;
        } catch (Throwable t) {
            LOG.error("Failed to load LicenseManager implementation", t);
            return null;
        }
    }
}
