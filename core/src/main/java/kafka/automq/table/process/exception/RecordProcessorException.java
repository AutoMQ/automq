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

public class RecordProcessorException extends Exception {
    /**
     * Constructs a new record processor exception with no detail message.
     */
    public RecordProcessorException() {
        super();
    }

    /**
     * Constructs a new record processor exception with the specified detail message.
     *
     * @param message the detail message explaining the cause of the exception
     */
    public RecordProcessorException(String message) {
        super(message);
    }

    /**
     * Constructs a new record processor exception with the specified detail message and cause.
     *
     * @param message the detail message explaining the cause of the exception
     * @param cause the cause of the exception (which is saved for later retrieval)
     */
    public RecordProcessorException(String message, Throwable cause) {
        super(message, cause);
    }

    /**
     * Constructs a new record processor exception with the specified cause.
     *
     * @param cause the cause of the exception (which is saved for later retrieval)
     */
    public RecordProcessorException(Throwable cause) {
        super(cause);
    }
}
