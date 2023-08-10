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
/*
 * Copyright 2017 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package kafka.metrics.cruisecontrol.metricsreporter.metric;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;

public final class ContainerMetricUtils {
    // A CPU quota value of -1 indicates that the cgroup does not adhere to any CPU time restrictions
    public static final int NO_CPU_QUOTA = -1;
    // Paths used to get cgroup information
    private static final String QUOTA_PATH = "/sys/fs/cgroup/cpu/cpu.cfs_quota_us";
    private static final String PERIOD_PATH = "/sys/fs/cgroup/cpu/cpu.cfs_period_us";
    // Unix command to execute inside a Linux container to get the number of logical processors available to the node
    private static final String NPROC = "nproc";

    private ContainerMetricUtils() {
    }

    /**
     * Reads cgroups CPU period from cgroups file. Value has a lowerbound of 1 millisecond and an upperbound of 1 second
     * according to https://www.kernel.org/doc/Documentation/scheduler/sched-bwc.txt
     *
     * @return Cgroups CPU period in microseconds as a double.
     */
    private static double getCpuPeriod() throws IOException {
        return Double.parseDouble(readFile(CgroupFiles.PERIOD_PATH.getValue()));
    }

    /**
     * Reads cgroups CPU quota from cgroups file. The value has a lowerbound of 1 millisecond
     * according to https://www.kernel.org/doc/Documentation/scheduler/sched-bwc.txt
     *
     * @return Cgroups CPU quota in microseconds as a double.
     */
    private static double getCpuQuota() throws IOException {
        return Double.parseDouble(readFile(CgroupFiles.QUOTA_PATH.getValue()));
    }

    /**
     * Gets the the number of logical cores available to the node.
     * <p>
     * We can get this value while running in a container by using the "nproc" command.
     * Using other methods like OperatingSystemMXBean.getAvailableProcessors() and
     * Runtime.getRuntime().availableProcessors() would require disabling container
     * support (-XX:-UseContainerSupport) since these methods are aware of container
     * boundaries
     *
     * @return Number of logical processors on node
     */
    private static int getAvailableProcessors() throws IOException {
        InputStream in = Runtime.getRuntime().exec(NPROC).getInputStream();
        return Integer.parseInt(readInputStream(in));
    }

    private static String readFile(String path) throws IOException {
        return readInputStream(new FileInputStream(path));
    }

    private static String readInputStream(InputStream in) throws IOException {
        BufferedReader br = new BufferedReader(new InputStreamReader(in, StandardCharsets.UTF_8));
        String stream = br.readLine();
        if (stream != null) {
            return stream;
        } else {
            throw new IllegalArgumentException("Nothing was read from stream " + in);
        }
    }

    /**
     * Get the "recent CPU usage" for the JVM process running inside of a container.
     * <p>
     * At this time, the methods of OperatingSystemMXBean used for retrieving recent CPU usage are not
     * container aware and calculate CPU usage with respect to the physical host instead of the operating
     * environment from which they are called from. There have been efforts to make these methods container
     * aware but the changes have not been backported to Java versions less than version 14.
     * <p>
     * Once these changes get backported, https://bugs.openjdk.java.net/browse/JDK-8226575, we can use
     * "getSystemCpuLoad()" for retrieving the CPU usage values when running in a container environment.
     *
     * @param cpuUtil The "recent CPU usage" for a JVM process with respect to node
     * @return the "recent CPU usage" for a JVM process with respect to operating environment
     * as a double in [0.0,1.0].
     */
    public static double getContainerProcessCpuLoad(double cpuUtil) throws IOException {
        int logicalProcessorsOfNode = getAvailableProcessors();
        double cpuQuota = getCpuQuota();
        if (cpuQuota == NO_CPU_QUOTA) {
            return cpuUtil;
        }

        // Get the number of CPUs of a node that can be used by the operating environment
        double cpuLimit = cpuQuota / getCpuPeriod();

        // Get the minimal number of CPUs needed to achieve the reported CPU utilization
        double cpus = cpuUtil * logicalProcessorsOfNode;

        /* Calculate the CPU utilization of a JVM process with respect to the operating environment.
         * Since the operating environment will only use the CPU resources allocated by CGroups,
         * it will always be that: cpuLimit >= cpus and the result is in the [0.0,1.0] interval.
         */
        return cpus / cpuLimit;
    }

    private enum CgroupFiles {
        QUOTA_PATH(ContainerMetricUtils.QUOTA_PATH),
        PERIOD_PATH(ContainerMetricUtils.PERIOD_PATH);

        private final String value;

        CgroupFiles(String value) {
            this.value = value;
        }

        private String getValue() {
            return value;
        }
    }
}
