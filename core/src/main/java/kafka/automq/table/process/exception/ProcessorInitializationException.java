/*
 * Copyright 2025, AutoMQ HK Limited.
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
package kafka.automq.table.process.exception;

/**
 * Unchecked exception for fatal, system-level errors during processor initialization.
 *
 * <p>This exception represents unrecoverable errors that occur during the setup
 * and configuration phase, such as:
 * <ul>
 *   <li>Failure to connect to a required external service (e.g., Schema Registry)</li>
 *   <li>Invalid or inconsistent configuration that prevents component creation</li>
 * </ul>
 *
 * <p>Unlike {@link RecordProcessorException}, this is a {@link RuntimeException}
 * because initialization errors are typically programming or deployment errors
 * that should not be caught and handled at runtime. They should cause the
 * application to fail fast.</p>
 */
public class ProcessorInitializationException extends RuntimeException {

    public ProcessorInitializationException(String message) {
        super(message);
    }

    public ProcessorInitializationException(String message, Throwable cause) {
        super(message, cause);
    }
}
