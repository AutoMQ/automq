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
package kafka.s3shell.util;

import com.automq.s3shell.sdk.util.S3PropUtil;
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
