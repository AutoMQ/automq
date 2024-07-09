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

import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Properties;
import kafka.tools.StorageTool;
import org.apache.commons.lang3.StringUtils;
import org.apache.kafka.server.config.ServerLogConfigs;

public class StorageUtil {

    public static void formatStorage(Properties serverProps, String configFilePath) {
        // auto format the storage if cluster.id is config
        String clusterId = serverProps.getProperty(AutoMQConfig.CLUSTER_ID_CONFIG);
        if (StringUtils.isBlank(clusterId)) {
            return;
        }
        String logDirPath = serverProps.getProperty(ServerLogConfigs.LOG_DIRS_CONFIG);
        if (!Files.exists(Paths.get(logDirPath, "meta.properties"))) {
            StorageTool.main(new String[] {"auto-format", "-t", clusterId, "-c=" + configFilePath});
        }
    }

}
