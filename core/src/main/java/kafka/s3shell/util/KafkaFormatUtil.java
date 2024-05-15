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
package kafka.s3shell.util;

import com.automq.shell.util.S3PropUtil;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Properties;
import kafka.tools.StorageTool;

public class KafkaFormatUtil {
    public static void formatStorage(String clusterId, Properties props) throws IOException {
        String propFileName = String.format("automq-%s.properties", clusterId);
        String propFilePath = "generated/" + propFileName;
        String logDirPath = props.getProperty("log.dirs");

        Path propPath = Paths.get(propFilePath);
        if (Files.exists(propPath)) {
            //delete if exists
            Files.delete(propPath);
        }

        S3PropUtil.persist(props, propFileName);
        if (!Files.isDirectory(Paths.get(logDirPath)) || !Files.exists(Paths.get(logDirPath, "meta.properties"))) {
            StorageTool.main(new String[] {"auto-format", "-t", clusterId, "-c=" + propFilePath});
        }
    }
}
