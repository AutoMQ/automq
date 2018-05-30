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
package org.apache.kafka.connect.runtime.errors;

/**
 * A logical stage in a Connect pipeline.
 */
public enum Stage {

    /**
     * When calling the poll() method on a SourceConnector
     */
    TASK_POLL,

    /**
     * When calling the put() method on a SinkConnector
     */
    TASK_PUT,

    /**
     * When running any transformation operation on a record
     */
    TRANSFORMATION,

    /**
     * When using the key converter to serialize/deserialize keys in ConnectRecords
     */
    KEY_CONVERTER,

    /**
     * When using the value converter to serialize/deserialize values in ConnectRecords
     */
    VALUE_CONVERTER,

    /**
     * When using the header converter to serialize/deserialize headers in ConnectRecords
     */
    HEADER_CONVERTER,

    /**
     * When producing to Kafka topic
     */
    KAFKA_PRODUCE,

    /**
     * When consuming from a Kafka topic
     */
    KAFKA_CONSUME
}
