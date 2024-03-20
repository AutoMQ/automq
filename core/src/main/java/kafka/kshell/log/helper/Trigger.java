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

package kafka.kshell.log.helper;



/**
 * @author ipsum-0320
 */
public class Trigger {
    // 使用 Trigger 类需要严格保证线程安全。
    private static Boolean isScan = false;

    public static void setScan(boolean isScan) {
        Trigger.isScan = isScan;
    }

    public static boolean getScan() {
        return Trigger.isScan;
    }
}
