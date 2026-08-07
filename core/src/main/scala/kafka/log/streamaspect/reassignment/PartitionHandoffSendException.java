/*
 * Copyright 2026, AutoMQ HK Limited.
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
package kafka.log.streamaspect.reassignment;

import java.util.Locale;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ExecutionException;

/** Reports why a partition handoff send did not complete successfully. */
public final class PartitionHandoffSendException extends RuntimeException {
    private final Reason reason;

    /**
     * Creates a handoff send exception without an underlying cause.
     *
     * @param reason stable reason used to select and log the fallback path
     */
    public PartitionHandoffSendException(Reason reason) {
        this(reason, null);
    }

    /**
     * Creates a handoff send exception with the failure that prevented delivery.
     *
     * @param reason stable reason used to select and log the fallback path
     * @param cause underlying send or timeout failure, or {@code null}
     */
    public PartitionHandoffSendException(Reason reason, Throwable cause) {
        super(reason.logValue(), cause);
        this.reason = reason;
    }

    /**
     * Returns the stable fallback reason.
     *
     * @return handoff failure reason
     */
    public Reason reason() {
        return reason;
    }

    /**
     * Unwraps asynchronous wrappers and normalizes an arbitrary failure as a handoff send exception.
     *
     * @param exception asynchronous or direct send failure
     * @return existing handoff send exception, or a send-failure wrapper
     */
    public static PartitionHandoffSendException from(Throwable exception) {
        Throwable cause = exception;
        while ((cause instanceof CompletionException || cause instanceof ExecutionException)
            && cause.getCause() != null) {
            cause = cause.getCause();
        }
        return cause instanceof PartitionHandoffSendException
            ? (PartitionHandoffSendException) cause
            : new PartitionHandoffSendException(Reason.SEND_FAILURE, cause);
    }

    public enum Reason {
        NOT_ATTEMPTED,
        SEND_FAILURE,
        SEND_TIMEOUT,
        HANDOFF_TOO_LARGE;

        /**
         * Returns the stable lowercase value used in reassignment logs.
         *
         * @return log field value
         */
        public String logValue() {
            return name().toLowerCase(Locale.ROOT);
        }
    }
}
