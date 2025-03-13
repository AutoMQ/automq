/*
 * Copyright 2024, AutoMQ HK Limited.
 *
 * The use of this file is governed by the Business Source License,
 * as detailed in the file "/LICENSE.S3Stream" included in this repository.
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */

package kafka.automq.interceptor;

import kafka.server.RequestLocal;

import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.record.MemoryRecords;
import org.apache.kafka.common.record.RecordValidationStats;
import org.apache.kafka.common.requests.ProduceResponse;

import java.util.Map;
import java.util.function.Consumer;

public class ProduceRequestArgs {
    private final short apiVersion;
    private final ClientIdMetadata clientId;
    private final int timeout;
    private final short requiredAcks;
    private final boolean internalTopicsAllowed;
    private final String transactionId;
    private final Map<TopicPartition, MemoryRecords> entriesPerPartition;
    private final Consumer<Map<TopicPartition, ProduceResponse.PartitionResponse>> responseCallback;
    private final Consumer<Map<TopicPartition, RecordValidationStats>> recordValidationStatsCallback;
    private final RequestLocal requestLocal;

    public ProduceRequestArgs(short apiVersion, ClientIdMetadata id, int timeout, short requiredAcks, boolean allowed,
        String transactionId, Map<TopicPartition, MemoryRecords> partition,
        Consumer<Map<TopicPartition, ProduceResponse.PartitionResponse>> callback,
        Consumer<Map<TopicPartition, RecordValidationStats>> statsCallback, RequestLocal local) {
        this.apiVersion = apiVersion;
        this.clientId = id;
        this.timeout = timeout;
        this.requiredAcks = requiredAcks;
        this.internalTopicsAllowed = allowed;
        this.transactionId = transactionId;
        this.entriesPerPartition = partition;
        this.responseCallback = callback;
        this.recordValidationStatsCallback = statsCallback;
        this.requestLocal = local;
    }

    public short apiVersion() {
        return apiVersion;
    }

    public ClientIdMetadata clientId() {
        return clientId;
    }

    public int timeout() {
        return timeout;
    }

    public short requiredAcks() {
        return requiredAcks;
    }

    public boolean internalTopicsAllowed() {
        return internalTopicsAllowed;
    }

    public String transactionId() {
        return transactionId;
    }

    public Map<TopicPartition, MemoryRecords> entriesPerPartition() {
        return entriesPerPartition;
    }

    public Consumer<Map<TopicPartition, ProduceResponse.PartitionResponse>> responseCallback() {
        return responseCallback;
    }

    public Consumer<Map<TopicPartition, RecordValidationStats>> recordValidationStatsCallback() {
        return recordValidationStatsCallback;
    }

    public RequestLocal requestLocal() {
        return requestLocal;
    }

    public Builder toBuilder() {
        return new Builder()
            .apiVersion(apiVersion)
            .clientId(clientId)
            .timeout(timeout)
            .requiredAcks(requiredAcks)
            .internalTopicsAllowed(internalTopicsAllowed)
            .transactionId(transactionId)
            .entriesPerPartition(entriesPerPartition)
            .responseCallback(responseCallback)
            .recordValidationStatsCallback(recordValidationStatsCallback)
            .requestLocal(requestLocal);
    }

    public static Builder builder() {
        return new Builder();
    }

    public static class Builder {
        private short apiVersion;
        private ClientIdMetadata clientId;
        private int timeout;
        private short requiredAcks;
        private boolean internalTopicsAllowed;
        private String transactionId;
        private Map<TopicPartition, MemoryRecords> entriesPerPartition;
        private Consumer<Map<TopicPartition, ProduceResponse.PartitionResponse>> responseCallback;
        private Consumer<Map<TopicPartition, RecordValidationStats>> recordValidationStatsCallback;
        private RequestLocal requestLocal;

        public Builder apiVersion(short apiVersion) {
            this.apiVersion = apiVersion;
            return this;
        }

        public Builder clientId(ClientIdMetadata clientId) {
            this.clientId = clientId;
            return this;
        }

        public Builder timeout(int timeout) {
            this.timeout = timeout;
            return this;
        }

        public Builder requiredAcks(short requiredAcks) {
            this.requiredAcks = requiredAcks;
            return this;
        }

        public Builder internalTopicsAllowed(boolean internalTopicsAllowed) {
            this.internalTopicsAllowed = internalTopicsAllowed;
            return this;
        }

        public Builder transactionId(String transactionId) {
            this.transactionId = transactionId;
            return this;
        }

        public Builder entriesPerPartition(Map<TopicPartition, MemoryRecords> entriesPerPartition) {
            this.entriesPerPartition = entriesPerPartition;
            return this;
        }

        public Builder responseCallback(Consumer<Map<TopicPartition, ProduceResponse.PartitionResponse>> responseCallback) {
            this.responseCallback = responseCallback;
            return this;
        }

        public Builder recordValidationStatsCallback(Consumer<Map<TopicPartition, RecordValidationStats>> recordValidationStatsCallback) {
            this.recordValidationStatsCallback = recordValidationStatsCallback;
            return this;
        }

        public Builder requestLocal(RequestLocal requestLocal) {
            this.requestLocal = requestLocal;
            return this;
        }

        public ProduceRequestArgs build() {
            return new ProduceRequestArgs(apiVersion, clientId, timeout, requiredAcks, internalTopicsAllowed, transactionId, entriesPerPartition, responseCallback, recordValidationStatsCallback, requestLocal);
        }
    }
}
