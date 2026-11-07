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
package com.automq.shell.util;

import java.io.File;
import java.io.IOException;
import java.io.PrintWriter;
import java.nio.charset.Charset;
import java.util.Enumeration;
import java.util.Properties;

public class S3PropUtil {
    public static final String BROKER_PROPS_PATH = "template/broker.properties";
    public static final String CONTROLLER_PROPS_PATH = "template/controller.properties";
    public static final String SERVER_PROPS_PATH = "template/server.properties";

    public static void persist(Properties props, String fileName) throws IOException {
        File directory = new File("generated");
        if (!directory.exists() && !directory.mkdirs()) {
            throw new IOException("Can't create directory " + directory.getAbsolutePath());
        }

        String targetPath = "generated/" + fileName;
        File file = new File(targetPath);
        try (PrintWriter pw = new PrintWriter(file, Charset.forName("utf-8"))) {
            for (Enumeration e = props.propertyNames(); e.hasMoreElements(); ) {
                String key = (String) e.nextElement();
                pw.println(key + "=" + props.getProperty(key));
            }
        }
    }

    public static Properties loadTemplateProps(String propsPath) throws IOException {
        try (var in = S3PropUtil.class.getClassLoader().getResourceAsStream(propsPath)) {
            if (in != null) {
                Properties props = new Properties();
                props.load(in);
                return props;
            } else {
                throw new IOException(String.format("Can not find resource file under path: %s", propsPath));
            }
        }
    }
}
