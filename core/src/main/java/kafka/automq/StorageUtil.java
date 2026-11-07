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

package kafka.automq;

import kafka.server.KafkaConfig;
import kafka.tools.StorageTool;

import org.apache.commons.lang3.StringUtils;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Properties;

public class StorageUtil {

    public static void formatStorage(Properties serverProps) {
        // auto format the storage if cluster.id is config
        String clusterId = serverProps.getProperty(AutoMQConfig.CLUSTER_ID_CONFIG);
        if (StringUtils.isBlank(clusterId)) {
            return;
        }
        String metadataLog = new KafkaConfig(serverProps, false).metadataLogDir();
        if (!Files.exists(Paths.get(metadataLog, "meta.properties"))) {
            String configFilePath = persistConfig(serverProps, metadataLog);
            StorageTool.execute(new String[] {"format", "-t", clusterId, "-c=" + configFilePath}, System.out);
        } else {
            persistConfig(serverProps, metadataLog);
        }
    }

    private static String persistConfig(Properties serverProps, String basePath) {
        String configFilePath = basePath + File.separator + "automq.properties";
        try {
            Path dir = Paths.get(basePath);
            if (!Files.exists(dir)) {
                Files.createDirectories(dir);
            }
            try (BufferedWriter writer = new BufferedWriter(new FileWriter(configFilePath, StandardCharsets.UTF_8))) {
                serverProps.store(writer, null);
                return configFilePath;
            }
        } catch (Throwable e) {
            throw new RuntimeException(e);
        }
    }

}
