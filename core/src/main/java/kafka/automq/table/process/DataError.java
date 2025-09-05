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

package kafka.automq.table.process;

import java.util.Objects;

/**
 * Represents a data-level error that occurred during record processing.
 *
 * <p>This class encapsulates recoverable errors that occur when processing
 * individual records. Unlike system-level exceptions, these errors allow
 * for more granular error handling strategies, such as:</p>
 * <ul>
 *   <li>Skipping bad records while continuing processing</li>
 *   <li>Logging detailed error information for later analysis</li>
 *   <li>Implementing custom retry logic for specific error types</li>
 *   <li>Routing failed records to dead letter queues</li>
 * </ul>
 *
 * <p>DataError instances are immutable and provide comprehensive error
 * information including type classification, human-readable messages,
 * and underlying causes.</p>
 *
 * @see ProcessingResult
 * @see ErrorType
 */
public final class DataError {

    private final ErrorType type;
    private final String message;
    private final Throwable cause;

    /**
     * Creates a new DataError with the specified type, message, and underlying cause.
     *
     * @param type the classification of this error
     * @param message a human-readable description of the error
     * @param cause the underlying exception that caused this error (may be null)
     * @throws IllegalArgumentException if type or message is null
     */
    public DataError(ErrorType type, String message, Throwable cause) {
        this.type = Objects.requireNonNull(type, "ErrorType cannot be null");
        this.message = Objects.requireNonNull(message, "Error message cannot be null");
        this.cause = cause;
    }

    /**
     * Returns the classification of this error.
     *
     * @return the error type, never null
     */
    public ErrorType getType() {
        return type;
    }

    /**
     * Returns a human-readable description of this error.
     *
     * @return the error message, never null
     */
    public String getMessage() {
        return message;
    }

    /**
     * Returns the underlying exception that caused this error.
     *
     * @return the cause exception, or null if there is no underlying cause
     */
    public Throwable getCause() {
        return cause;
    }

    /**
     * Checks if this error has an underlying cause exception.
     *
     * @return true if there is an underlying cause, false otherwise
     */
    public boolean hasCause() {
        return cause != null;
    }

    /**
     * Creates a detailed error message that includes type, message, and cause information.
     *
     * @return a comprehensive error description
     */
    public String getDetailedMessage() {
        StringBuilder sb = new StringBuilder();
        sb.append("[").append(type).append("] ").append(message);
        if (hasCause()) {
            sb.append(" (caused by: ").append(cause.getClass().getSimpleName());
            if (cause.getMessage() != null) {
                sb.append(" - ").append(cause.getMessage());
            }
            sb.append(")");
        }
        return sb.toString();
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) return true;
        if (obj == null || getClass() != obj.getClass()) return false;

        DataError dataError = (DataError) obj;
        return type == dataError.type &&
               Objects.equals(message, dataError.message) &&
               Objects.equals(cause, dataError.cause);
    }

    @Override
    public int hashCode() {
        return Objects.hash(type, message, cause);
    }

    @Override
    public String toString() {
        return "DataError{" +
                "type=" + type +
                ", message='" + message + '\'' +
                ", cause=" + (cause != null ? cause.getClass().getSimpleName() : "null") +
                '}';
    }

    /**
     * Classification of data processing errors for appropriate error handling.
     */
    public enum ErrorType {
        /**
         * Indicates that the record's data is malformed or invalid.
         * This can be due to a null payload, incorrect data size, or a missing or unknown magic byte that prevents further processing.
         */
        DATA_ERROR,
        /**
         * Indicates a failure during the data deserialization or conversion step.
         * This can be caused by an inability to fetch a schema from a registry, serialization errors (e.g., Avro, Protobuf), or other schema-related failures.
         */
        CONVERT_ERROR,
        /**
         * Indicates a failure within a data transformation step.
         * This can be due to the input data not matching the format expected by a transform (e.g., an invalid Debezium record).
         */
        TRANSFORMATION_ERROR,

        /**
         *
         */
        SYSTEM_ERROR,

        /**
         * Unknown exception during processing.
         */
        UNKNOW_ERROR
    }
}
