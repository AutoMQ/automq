package org.apache.kafka.common.errors;

public class InvalidKVRecordEpochException extends ApiException {

    public InvalidKVRecordEpochException(String message) {
        super(message);
    }
}
