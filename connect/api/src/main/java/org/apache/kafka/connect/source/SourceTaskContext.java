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
package org.apache.kafka.connect.source;

import org.apache.kafka.connect.storage.OffsetStorageReader;

import java.util.Map;

/**
 * SourceTaskContext is provided to SourceTasks to allow them to interact with the underlying
 * runtime.
 */
public interface SourceTaskContext {
    /**
     * Get the Task configuration.  This is the latest configuration and may differ from that passed on startup.
     *
     * For example, this method can be used to obtain the latest configuration if an external secret has changed,
     * and the configuration is using variable references such as those compatible with
     * {@link org.apache.kafka.common.config.ConfigTransformer}.
     */
    Map<String, String> configs();

    /**
     * Get the OffsetStorageReader for this SourceTask.
     */
    OffsetStorageReader offsetStorageReader();
}
