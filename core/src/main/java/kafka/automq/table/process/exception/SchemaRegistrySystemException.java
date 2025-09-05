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


import io.confluent.kafka.schemaregistry.client.rest.exceptions.RestClientException;

/**
 * Exception representing system-level errors when interacting with the Schema Registry.
 * These are critical errors that typically cannot be tolerated even with ErrorsTolerance=ALL
 * configuration, as they represent system infrastructure issues rather than data problems.
 */
public class SchemaRegistrySystemException extends RuntimeException {

    private final ErrorType errorType;
    private final int statusCode;

    /**
     * Classification of schema registry system errors
     */
    public enum ErrorType {
        /**
         * Authentication errors (401)
         */
        AUTHENTICATION_ERROR,

        /**
         * Authorization errors (403)
         */
        AUTHORIZATION_ERROR,

        /**
         * Rate limiting errors (429)
         */
        RATE_LIMIT_ERROR,

        /**
         * Temporary service unavailable errors (408, 503, 504)
         */
        SERVICE_UNAVAILABLE_ERROR,

        /**
         * Gateway errors (502) indicating upstream service issues
         */
        GATEWAY_ERROR,

        /**
         * Other unexpected system errors
         */
        UNKNOWN_SYSTEM_ERROR
    }

    public SchemaRegistrySystemException(String message, Throwable cause, ErrorType errorType, int statusCode) {
        super(message, cause);
        this.errorType = errorType;
        this.statusCode = statusCode;
    }

    public ErrorType getErrorType() {
        return errorType;
    }

    public int getStatusCode() {
        return statusCode;
    }



    // io.confluent.kafka.serializers.AbstractKafkaSchemaSerDe#toKafkaException
    public static SchemaRegistrySystemException fromStatusCode(RestClientException exception, String recordContext) {
        int status = exception.getStatus();

        if (status == 401) {
            return new SchemaRegistrySystemException(
                "Authentication error when accessing schema registry for record: " + recordContext,
                exception, ErrorType.AUTHENTICATION_ERROR, status);
        } else if (status == 403) {
            return new SchemaRegistrySystemException(
                "Authorization error when accessing schema registry for record: " + recordContext,
                exception, ErrorType.AUTHORIZATION_ERROR, status);
        } else if (status == 429) {  // Too Many Requests
            return new SchemaRegistrySystemException(
                "Rate limit exceeded when accessing schema registry for record: " + recordContext,
                exception, ErrorType.RATE_LIMIT_ERROR, status);
        } else if (status == 408    // Request Timeout
            || status == 503        // Service Unavailable
            || status == 504) {     // Gateway Timeout
            return new SchemaRegistrySystemException(
                "Service unavailable or timeout when accessing schema registry for record: " + recordContext,
                exception, ErrorType.SERVICE_UNAVAILABLE_ERROR, status);
        } else if (status == 502) { // Bad Gateway
            return new SchemaRegistrySystemException(
                "Bad gateway error when accessing schema registry for record: " + recordContext,
                exception, ErrorType.GATEWAY_ERROR, status);
        } else {
            return new SchemaRegistrySystemException(
                "Unexpected schema registry error (HTTP " + status + ") for record: " + recordContext,
                exception, ErrorType.UNKNOWN_SYSTEM_ERROR, status);
        }
    }
}
