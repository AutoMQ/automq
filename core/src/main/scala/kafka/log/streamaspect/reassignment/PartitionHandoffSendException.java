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

    public PartitionHandoffSendException(Reason reason) {
        this(reason, null);
    }

    public PartitionHandoffSendException(Reason reason, Throwable cause) {
        super(reason.logValue(), cause);
        this.reason = reason;
    }

    public Reason reason() {
        return reason;
    }

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

        public String logValue() {
            return name().toLowerCase(Locale.ROOT);
        }
    }
}
