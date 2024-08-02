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

package com.automq.shell.metrics;

import org.apache.commons.lang3.StringUtils;

public class PrometheusUtils {
    private static final String TOTAL_SUFFIX = "_total";

    public static String getPrometheusUnit(String unit) {
        if (unit.contains("{")) {
            return "";
        }
        switch (unit) {
            // Time
            case "d":
                return "days";
            case "h":
                return "hours";
            case "min":
                return "minutes";
            case "s":
                return "seconds";
            case "ms":
                return "milliseconds";
            case "us":
                return "microseconds";
            case "ns":
                return "nanoseconds";
            // Bytes
            case "By":
                return "bytes";
            case "KiBy":
                return "kibibytes";
            case "MiBy":
                return "mebibytes";
            case "GiBy":
                return "gibibytes";
            case "TiBy":
                return "tibibytes";
            case "KBy":
                return "kilobytes";
            case "MBy":
                return "megabytes";
            case "GBy":
                return "gigabytes";
            case "TBy":
                return "terabytes";
            // SI
            case "m":
                return "meters";
            case "V":
                return "volts";
            case "A":
                return "amperes";
            case "J":
                return "joules";
            case "W":
                return "watts";
            case "g":
                return "grams";
            // Misc
            case "Cel":
                return "celsius";
            case "Hz":
                return "hertz";
            case "1":
                return "";
            case "%":
                return "percent";
            default:
                return unit;
        }
    }

    public static String mapMetricsName(String name, String unit, boolean isCounter, boolean isGauge) {
        // Replace "." into "_"
        name = name.replaceAll("\\.", "_");

        String prometheusUnit = getPrometheusUnit(unit);
        boolean shouldAppendUnit = StringUtils.isNotBlank(prometheusUnit) && !name.contains(prometheusUnit);

        // append prometheus unit if not null or empty.
        // unit should be appended before type suffix
        if (shouldAppendUnit) {
            name = name + "_" + prometheusUnit;
        }

        // trim counter's _total suffix so the unit is placed before it.
        if (isCounter && name.endsWith(TOTAL_SUFFIX)) {
            name = name.substring(0, name.length() - TOTAL_SUFFIX.length());
        }

        // replace _total suffix, or add if it wasn't already present.
        if (isCounter) {
            name = name + TOTAL_SUFFIX;
        }
        // special case - gauge
        if (unit.equals("1") && isGauge && !name.contains("ratio")) {
            name = name + "_ratio";
        }
        return name;
    }

    public static String mapLabelName(String name) {
        if (StringUtils.isBlank(name)) {
            return "";
        }
        return name.replaceAll("\\.", "_");
    }
}
