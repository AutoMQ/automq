/*
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

package kafka.log.s3.compact;

import kafka.log.s3.compact.objects.CompactedObject;
import kafka.log.s3.compact.objects.StreamDataBlock;

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
