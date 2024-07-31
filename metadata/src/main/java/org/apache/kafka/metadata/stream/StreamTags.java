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

package org.apache.kafka.metadata.stream;

import org.apache.kafka.common.Uuid;

public class StreamTags {

    public static class Topic {
        public static final String KEY = "0";

        public static String encode(Uuid topicId) {
            return topicId.toString();
        }

        public static Uuid decode(String tag) {
            return Uuid.fromString(tag);
        }
    }

    public static class Partition {
        public static final String KEY = "1";

        public static String encode(int partition) {
            return Integer.toHexString(partition);
        }

        public static int decode(String tag) {
            return Integer.parseInt(tag, 16);
        }

    }
}
