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

package kafka.autobalancer.common;

import java.text.DecimalFormat;

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

}
