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

package com.automq.shell.commands.cluster;

import org.apache.kafka.common.Uuid;

import com.google.common.io.Resources;

import java.io.File;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.concurrent.Callable;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import picocli.CommandLine;

@CommandLine.Command(name = "create", description = "Create a AutoMQ cluster project", mixinStandardHelpOptions = true)
public class Create implements Callable<Integer> {
    @CommandLine.Parameters(index = "0", description = "cluster name")
    private String clusterName;

    @Override
    public Integer call() throws Exception {
        String topoTemplate = Resources.toString(Resources.getResource("template" + File.separator + "topo.yaml"), StandardCharsets.UTF_8);
        // replace cluster id
        Pattern pattern = Pattern.compile("(clusterId: '')", Pattern.DOTALL);
        Matcher matcher = pattern.matcher(topoTemplate);
        String clusterId = Uuid.randomUuid().toString();
        String newContent = matcher.replaceFirst("clusterId: '" + clusterId + "'");
        // Create a directory with the name specified by clusterName in the current directory
        String newClusterTopoPath = "clusters" + File.separator + clusterName + File.separator + "topo.yaml";
        if (Files.exists(Paths.get(newClusterTopoPath))) {
            System.out.printf("The cluster[%s] is already exists, please specify another cluster name\n", clusterName);
            return 1;
        }
        Files.createDirectories(Paths.get("clusters" + File.separator + clusterName));
        Files.write(Paths.get(newClusterTopoPath), newContent.getBytes(StandardCharsets.UTF_8));
        System.out.printf("Success create AutoMQ cluster project: %s\n", clusterName);
        System.out.println("========================================================");
        System.out.print("Please follow the steps to deploy AutoMQ cluster:\n");
        System.out.printf("1. Modify the cluster topology config %s to fit your needs\n", newClusterTopoPath);
        System.out.printf("2. Run ./bin/automq-cli.sh cluster deploy --dry-run clusters/%s , to deploy the AutoMQ cluster\n", clusterName);
        return 0;
    }
}
