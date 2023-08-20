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
package org.apache.kafka.tiered.storage.actions;

import org.apache.kafka.tiered.storage.TieredStorageTestAction;
import org.apache.kafka.tiered.storage.TieredStorageTestContext;
import org.apache.kafka.clients.admin.RecordsToDelete;
import org.apache.kafka.common.TopicPartition;

import java.io.PrintStream;
import java.util.Collections;
import java.util.Map;
import java.util.concurrent.ExecutionException;

public final class DeleteRecordsAction implements TieredStorageTestAction {

    private final TopicPartition partition;
    private final Long beforeOffset;

    public DeleteRecordsAction(TopicPartition partition,
                               Long beforeOffset) {
        this.partition = partition;
        this.beforeOffset = beforeOffset;
    }

    @Override
    public void doExecute(TieredStorageTestContext context) throws InterruptedException, ExecutionException {
        Map<TopicPartition, RecordsToDelete> recordsToDeleteMap =
                Collections.singletonMap(partition, RecordsToDelete.beforeOffset(beforeOffset));
        context.admin().deleteRecords(recordsToDeleteMap).all().get();
    }

    @Override
    public void describe(PrintStream output) {
        output.printf("delete-records partition: %s, before-offset: %d%n", partition, beforeOffset);
    }
}
