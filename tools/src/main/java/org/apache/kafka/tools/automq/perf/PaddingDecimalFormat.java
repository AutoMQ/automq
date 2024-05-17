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

package org.apache.kafka.tools.automq.perf;

import java.text.DecimalFormat;
import java.text.FieldPosition;

public class PaddingDecimalFormat extends DecimalFormat {
    private final int minimumLength;

    /**
     * Creates a PaddingDecimalFormat using the given pattern and minimum minimumLength and the
     * symbols for the default locale.
     */
    public PaddingDecimalFormat(String pattern, int minLength) {
        super(pattern);
        minimumLength = minLength;
    }

    @Override
    public StringBuffer format(double number, StringBuffer toAppendTo, FieldPosition pos) {
        int initLength = toAppendTo.length();
        super.format(number, toAppendTo, pos);
        return pad(toAppendTo, initLength);
    }

    @Override
    public StringBuffer format(long number, StringBuffer toAppendTo, FieldPosition pos) {
        int initLength = toAppendTo.length();
        super.format(number, toAppendTo, pos);
        return pad(toAppendTo, initLength);
    }

    private StringBuffer pad(StringBuffer toAppendTo, int initLength) {
        int numLength = toAppendTo.length() - initLength;
        int padLength = minimumLength - numLength;
        if (padLength > 0) {
            toAppendTo.insert(initLength, " ".repeat(padLength));
        }
        return toAppendTo;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (obj == null || getClass() != obj.getClass()) {
            return false;
        }
        PaddingDecimalFormat that = (PaddingDecimalFormat) obj;
        return minimumLength == that.minimumLength;
    }

    @Override
    public int hashCode() {
        return 31 * super.hashCode() + minimumLength;
    }
}
