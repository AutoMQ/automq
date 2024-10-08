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

package com.automq.stream.s3.compact;

import com.automq.stream.s3.StreamDataBlock;
import com.automq.stream.s3.compact.objects.CompactedObject;

import java.util.List;
import java.util.Map;

public class CompactionPlan {
    private final int order;
    private final List<CompactedObject> compactedObjects;
    private final Map<Long/* Object id*/, List<StreamDataBlock>> streamDataBlocksMap;

    public CompactionPlan(int order, List<CompactedObject> compactedObjects,
        Map<Long, List<StreamDataBlock>> streamDataBlocksMap) {
        this.order = order;
        this.compactedObjects = compactedObjects;
        this.streamDataBlocksMap = streamDataBlocksMap;
    }

    public int order() {
        return order;
    }

    public List<CompactedObject> compactedObjects() {
        return compactedObjects;
    }

    public Map<Long, List<StreamDataBlock>> streamDataBlocksMap() {
        return streamDataBlocksMap;
    }
}
