/*
 * Copyright 2024, AutoMQ HK Limited.
 *
 * The use of this file is governed by the Business Source License,
 * as detailed in the file "/LICENSE.S3Stream" included in this repository.
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */

package kafka.autobalancer.common;

import java.text.DecimalFormat;
import java.util.Optional;
import org.apache.kafka.common.metadata.BrokerRegistrationChangeRecord;
import org.apache.kafka.common.metadata.RegisterBrokerRecord;
import org.apache.kafka.metadata.BrokerRegistrationFencingChange;
import org.apache.kafka.metadata.BrokerRegistrationInControlledShutdownChange;

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
