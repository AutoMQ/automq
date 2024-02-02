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

package com.automq.stream.s3.compact;

import com.automq.stream.s3.StreamDataBlock;
import com.automq.stream.s3.compact.objects.CompactedObject;
import java.util.List;
import java.util.Map;

public class CompactionPlan {
    private final List<CompactedObject> compactedObjects;
    private final Map<Long/* Object id*/, List<StreamDataBlock>> streamDataBlocksMap;

    public CompactionPlan(List<CompactedObject> compactedObjects,
        Map<Long, List<StreamDataBlock>> streamDataBlocksMap) {
        this.compactedObjects = compactedObjects;
        this.streamDataBlocksMap = streamDataBlocksMap;
    }

    public List<CompactedObject> compactedObjects() {
        return compactedObjects;
    }

    public Map<Long, List<StreamDataBlock>> streamDataBlocksMap() {
        return streamDataBlocksMap;
    }
}
