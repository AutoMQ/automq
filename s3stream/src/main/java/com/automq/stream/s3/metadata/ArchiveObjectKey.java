/*
 * Copyright 2026, AutoMQ HK Limited.
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

package com.automq.stream.s3.metadata;

import java.util.Locale;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Defines the deterministic object-storage namespace for archived Composite manifests.
 */
public final class ArchiveObjectKey {
    private static final Pattern MANIFEST_KEY_PATTERN = Pattern.compile(
        "archive/(0|[1-9][0-9]*)/([0-9]{19})-([0-9]{19})-(0|[1-9][0-9]*)-(0|[1-9][0-9]*)");

    private ArchiveObjectKey() {
    }

    /**
     * Build an end-offset-ordered Archive manifest key.
     *
     * @param streamId Stream identity
     * @param startOffset inclusive represented offset
     * @param endOffset exclusive represented offset
     * @param objectId reused Composite object identity
     * @param logicalSize logical bytes represented by the manifest indexes
     * @return deterministic Archive manifest key
     */
    public static String manifestKey(long streamId, long startOffset, long endOffset, long objectId,
        long logicalSize) {
        if (streamId < 0 || startOffset < 0 || endOffset < startOffset || objectId < 0 || logicalSize < 0) {
            throw new IllegalArgumentException("Archive key fields must be non-negative and offsets ordered");
        }
        return String.format(Locale.ROOT, "archive/%d/%019d-%019d-%d-%d", streamId, endOffset, startOffset,
            objectId, logicalSize);
    }

    /**
     * Build the Archive manifest prefix for one Stream.
     *
     * @param streamId Stream identity
     * @return Archive manifest prefix
     */
    public static String manifestPrefix(long streamId) {
        if (streamId < 0) {
            throw new IllegalArgumentException("Stream ID must be non-negative");
        }
        return "archive/" + streamId + "/";
    }

    /**
     * Build an exclusive ordered-LIST cursor that skips manifests ending at or before the requested offset.
     *
     * @param streamId Stream identity
     * @param offset requested Fetch offset
     * @return exclusive ordered-LIST cursor
     */
    public static String startAfter(long streamId, long offset) {
        if (offset < 0) {
            throw new IllegalArgumentException("Offset must be non-negative");
        }
        return String.format(Locale.ROOT, "%s%019d~", manifestPrefix(streamId), offset);
    }

    /**
     * Parse and validate one canonical Archive manifest key.
     *
     * @param key storage key
     * @return parsed manifest identity, range, and logical size
     * @throws IllegalArgumentException if the key is outside the Archive namespace or is not canonical
     */
    public static ManifestKey parseManifestKey(String key) {
        Matcher matcher = MANIFEST_KEY_PATTERN.matcher(key);
        if (!matcher.matches()) {
            throw new IllegalArgumentException("Malformed Archive manifest key: " + key);
        }
        try {
            long streamId = Long.parseLong(matcher.group(1));
            long endOffset = Long.parseLong(matcher.group(2));
            long startOffset = Long.parseLong(matcher.group(3));
            long objectId = Long.parseLong(matcher.group(4));
            long logicalSize = Long.parseLong(matcher.group(5));
            if (endOffset <= startOffset) {
                throw new IllegalArgumentException("Archive manifest range must be non-empty: " + key);
            }
            return new ManifestKey(streamId, startOffset, endOffset, objectId, logicalSize);
        } catch (NumberFormatException e) {
            throw new IllegalArgumentException("Archive manifest key contains an out-of-range number: " + key, e);
        }
    }

    /**
     * Canonical fields encoded in an Archive manifest key.
     */
    public record ManifestKey(long streamId, long startOffset, long endOffset, long objectId, long logicalSize) {
    }
}
