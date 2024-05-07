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

package org.apache.kafka.controller.stream;

public class TopicDeletion {

    public static final String TOPIC_DELETION_PREFIX = "__automq_topic_deletion/";

    public enum Status {
        PENDING(0),
        IN_PROGRESS(1),
        COMPLETE(2);

        private final int value;

        Status(int value) {
            this.value = value;
        }

        public int value() {
            return value;
        }

        public static Status forValue(int value) {
            switch (value) {
                case 0:
                    return PENDING;
                case 1:
                    return IN_PROGRESS;
                case 2:
                    return COMPLETE;
                default:
                    throw new IllegalArgumentException("Invalid value " + value);
            }
        }
    }

}
