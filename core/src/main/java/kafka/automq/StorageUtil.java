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

package kafka.automq;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Properties;
import kafka.server.KafkaConfig;
import kafka.tools.StorageTool;
import org.apache.commons.lang3.StringUtils;

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
            StorageTool.main(new String[] {"auto-format", "-t", clusterId, "-c=" + configFilePath});
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
