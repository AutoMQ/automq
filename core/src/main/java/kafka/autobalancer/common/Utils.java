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

package kafka.autobalancer.common;

import org.apache.kafka.common.metadata.BrokerRegistrationChangeRecord;
import org.apache.kafka.common.metadata.RegisterBrokerRecord;
import org.apache.kafka.metadata.BrokerRegistrationFencingChange;
import org.apache.kafka.metadata.BrokerRegistrationInControlledShutdownChange;

import java.text.DecimalFormat;
import java.util.Optional;

public class Utils {

    public static final long BYTE = 1L;
    public static final long KB = BYTE << 10;
    public static final long MB = KB << 10;
    public static final long GB = MB << 10;
    public static final long TB = GB << 10;
    public static final long PB = TB << 10;
    public static final long EB = PB << 10;

    private static final DecimalFormat DEC_FORMAT = new DecimalFormat("#.##");

    private static String formatSize(long size, long divider, String unitName) {
        return DEC_FORMAT.format((double) size / divider) + unitName;
    }

    public static String formatDataSize(long size) {
        if (size < 0)
            return String.valueOf(size);
        if (size >= EB)
            return formatSize(size, EB, "EB");
        if (size >= PB)
            return formatSize(size, PB, "PB");
        if (size >= TB)
            return formatSize(size, TB, "TB");
        if (size >= GB)
            return formatSize(size, GB, "GB");
        if (size >= MB)
            return formatSize(size, MB, "MB");
        if (size >= KB)
            return formatSize(size, KB, "KB");
        return formatSize(size, BYTE, "Bytes");
    }

    public static boolean checkListenerName(String listenerName, String expect) {
        if (listenerName == null || listenerName.isEmpty() || listenerName.equals("CONTROLLER")) {
            return false;
        }
        return expect.isEmpty() || listenerName.equals(expect);
    }

    public static Optional<Boolean> isBrokerFenced(BrokerRegistrationChangeRecord record) {
        BrokerRegistrationFencingChange fencingChange =
            BrokerRegistrationFencingChange.fromValue(record.fenced()).orElseThrow(
                () -> new IllegalStateException(String.format("Unable to replay %s: unknown " +
                    "value for fenced field: %x", record, record.fenced())));
        BrokerRegistrationInControlledShutdownChange inControlledShutdownChange =
            BrokerRegistrationInControlledShutdownChange.fromValue(record.inControlledShutdown()).orElseThrow(
                () -> new IllegalStateException(String.format("Unable to replay %s: unknown " +
                    "value for inControlledShutdown field: %x", record, record.inControlledShutdown())));
        if (fencingChange == BrokerRegistrationFencingChange.FENCE
            || inControlledShutdownChange == BrokerRegistrationInControlledShutdownChange.IN_CONTROLLED_SHUTDOWN) {
            return Optional.of(true);
        } else if (fencingChange == BrokerRegistrationFencingChange.UNFENCE) {
            return Optional.of(false);
        } else {
            // broker status unchanged
            return Optional.empty();
        }
    }

    public static boolean isBrokerFenced(RegisterBrokerRecord record) {
        return record.fenced() || record.inControlledShutdown();
    }
}
