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
package com.automq.stream.thirdparty.moe.cnkirito.kdio;

/**
 * Constants for {@link DirectIOLib#oDirectOpen(String, boolean)}.
 */
public interface OpenFlags {
    OpenFlags INSTANCE = instance();

    private static OpenFlags instance() {
        String arch = System.getProperty("os.arch");
        switch (arch) {
            case "aarch64":
                return new Aarch64OpenFlags();
            default:
                return new DefaultOpenFlags();
        }
    }

    default int oRDONLY() {
        return 00;
    }
    default int oWRONLY() {
        return 01;
    }
    default int oRDWR() {
        return 02;
    }
    default int oCREAT() {
        return 0100;
    }
    default int oTRUNC() {
        return 01000;
    }
    default int oDIRECT() {
        return 040000;
    }
    default int oSYNC() {
        return 04010000;
    }

    class DefaultOpenFlags implements OpenFlags {
    }

    class Aarch64OpenFlags implements OpenFlags {
        @Override
        public int oDIRECT() {
            return 0200000;
        }
    }
}
