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
package org.apache.kafka.streams.test;

import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.streams.TopologyTestDriver;

import java.util.Objects;

/**
 * Helper class to verify topology result records.
 *
 * @see TopologyTestDriver
 */
public class OutputVerifier {

    /**
     * Compares a {@link ProducerRecord} with the provided value and throws an {@link AssertionError} if the
     * {@code ProducerRecord}'s value is not equal to the expected value.
     *
     * @param record a output {@code ProducerRecord} for verification
     * @param expectedValue the expected value of the {@code ProducerRecord}
     * @param <K> the key type
     * @param <V> the value type
     * @throws AssertionError if {@code ProducerRecord}'s value is not equal to {@code expectedValue}
     */
    public static <K, V> void compareValue(final ProducerRecord<K, V> record,
                                           final V expectedValue) throws AssertionError {
        Objects.requireNonNull(record);

        final V recordValue = record.value();
        final AssertionError error = new AssertionError("Expected value=" + expectedValue + " but was value=" + recordValue);

        if (recordValue != null) {
            if (!recordValue.equals(expectedValue)) {
                throw error;
            }
        } else if (expectedValue != null) {
            throw error;
        }
    }

    /**
     * Compares the values of two {@link ProducerRecord}'s and throws an {@link AssertionError} if they are not equal to
     * each other.
     *
     * @param record a output {@code ProducerRecord} for verification
     * @param expectedRecord a {@code ProducerRecord} for verification
     * @param <K> the key type
     * @param <V> the value type
     * @throws AssertionError if {@code ProducerRecord}'s value is not equal to {@code expectedRecord}'s value
     */
    public static <K, V> void compareValue(final ProducerRecord<K, V> record,
                                           final ProducerRecord<K, V> expectedRecord) throws AssertionError {
        Objects.requireNonNull(expectedRecord);
        compareValue(record, expectedRecord.value());
    }

    /**
     * Compares a {@link ProducerRecord} with the provided key and value and throws an {@link AssertionError} if the
     * {@code ProducerRecord}'s key or value is not equal to the expected key or value.
     *
     * @param record a output {@code ProducerRecord} for verification
     * @param expectedKey the expected key of the {@code ProducerRecord}
     * @param expectedValue the expected value of the {@code ProducerRecord}
     * @param <K> the key type
     * @param <V> the value type
     * @throws AssertionError if {@code ProducerRecord}'s key or value is not equal to {@code expectedKey} or {@code expectedValue}
     */
    public static <K, V> void compareKeyValue(final ProducerRecord<K, V> record,
                                              final K expectedKey,
                                              final V expectedValue) throws AssertionError {
        Objects.requireNonNull(record);

        final K recordKey = record.key();
        final V recordValue = record.value();
        final AssertionError error = new AssertionError("Expected <" + expectedKey + ", " + expectedValue + "> " +
            "but was <" + recordKey + ", " + recordValue + ">");

        if (recordKey != null) {
            if (!recordKey.equals(expectedKey)) {
                throw error;
            }
        } else if (expectedKey != null) {
            throw error;
        }

        if (recordValue != null) {
            if (!recordValue.equals(expectedValue)) {
                throw error;
            }
        } else if (expectedValue != null) {
            throw error;
        }
    }

    /**
     * Compares the keys and values of two {@link ProducerRecord}'s and throws an {@link AssertionError} if the keys or
     * values are not equal to each other.
     *
     * @param record a output {@code ProducerRecord} for verification
     * @param expectedRecord a {@code ProducerRecord} for verification
     * @param <K> the key type
     * @param <V> the value type
     * @throws AssertionError if {@code ProducerRecord}'s key or value is not equal to {@code expectedRecord}'s key or value
     */
    public static <K, V> void compareKeyValue(final ProducerRecord<K, V> record,
                                              final ProducerRecord<K, V> expectedRecord) throws AssertionError {
        Objects.requireNonNull(expectedRecord);
        compareKeyValue(record, expectedRecord.key(), expectedRecord.value());
    }

    /**
     * Compares a {@link ProducerRecord} with the provided value and timestamp and throws an {@link AssertionError} if
     * the {@code ProducerRecord}'s value or timestamp is not equal to the expected value or timestamp.
     *
     * @param record a output {@code ProducerRecord} for verification
     * @param expectedValue the expected value of the {@code ProducerRecord}
     * @param expectedTimestamp the expected timestamps of the {@code ProducerRecord}
     * @param <K> the key type
     * @param <V> the value type
     * @throws AssertionError if {@code ProducerRecord}'s value or timestamp is not equal to {@code expectedValue} or {@code expectedTimestamp}
     */
    public static <K, V> void compareValueTimestamp(final ProducerRecord<K, V> record,
                                                    final V expectedValue,
                                                    final long expectedTimestamp) throws AssertionError {
        Objects.requireNonNull(record);

        final V recordValue = record.value();
        final long recordTimestamp = record.timestamp();
        final AssertionError error = new AssertionError("Expected value=" + expectedValue + " with timestamp=" + expectedTimestamp +
            " but was value=" + recordValue + " with timestamp=" + recordTimestamp);

        if (recordValue != null) {
            if (!recordValue.equals(expectedValue)) {
                throw error;
            }
        } else if (expectedValue != null) {
            throw error;
        }

        if (recordTimestamp != expectedTimestamp) {
            throw error;
        }
    }

    /**
     * Compares the values and timestamps of two {@link ProducerRecord}'s and throws an {@link AssertionError} if the
     * values or timestamps are not equal to each other.
     *
     * @param record a output {@code ProducerRecord} for verification
     * @param expectedRecord a {@code ProducerRecord} for verification
     * @param <K> the key type
     * @param <V> the value type
     * @throws AssertionError if {@code ProducerRecord}'s value or timestamp is not equal to {@code expectedRecord}'s value or timestamp
     */
    public static <K, V> void compareValueTimestamp(final ProducerRecord<K, V> record,
                                                    final ProducerRecord<K, V> expectedRecord) throws AssertionError {
        Objects.requireNonNull(expectedRecord);
        compareValueTimestamp(record, expectedRecord.value(), expectedRecord.timestamp());
    }

    /**
     * Compares a {@link ProducerRecord} with the provided key, value, and timestamp and throws an
     * {@link AssertionError} if the {@code ProducerRecord}'s key, value, or timestamp is not equal to the expected key,
     * value, or timestamp.
     *
     * @param record a output {@code ProducerRecord} for verification
     * @param expectedKey the expected key of the {@code ProducerRecord}
     * @param expectedValue the expected value of the {@code ProducerRecord}
     * @param expectedTimestamp the expected timestamp of the {@code ProducerRecord}
     * @param <K> the key type
     * @param <V> the value type
     * @throws AssertionError if {@code ProducerRecord}'s key, value, timestamp is not equal to {@code expectedKey},
     * {@code expectedValue}, or {@code expectedTimestamps}
     */
    public static <K, V> void compareKeyValueTimestamp(final ProducerRecord<K, V> record,
                                                       final K expectedKey,
                                                       final V expectedValue,
                                                       final long expectedTimestamp) throws AssertionError {
        Objects.requireNonNull(record);

        final K recordKey = record.key();
        final V recordValue = record.value();
        final long recordTimestamp = record.timestamp();
        final AssertionError error = new AssertionError("Expected <" + expectedKey + ", " + expectedValue + "> with timestamp=" + expectedTimestamp +
            " but was <" + recordKey + ", " + recordValue + "> with timestamp=" + recordTimestamp);

        if (recordKey != null) {
            if (!recordKey.equals(expectedKey)) {
                throw error;
            }
        } else if (expectedKey != null) {
            throw error;
        }

        if (recordValue != null) {
            if (!recordValue.equals(expectedValue)) {
                throw error;
            }
        } else if (expectedValue != null) {
            throw error;
        }

        if (recordTimestamp != expectedTimestamp) {
            throw error;
        }
    }

    /**
     * Compares the keys, values, and timestamps of two {@link ProducerRecord}'s and throws an {@link AssertionError} if
     * the keys, values, or timestamps are not equal to each other.
     *
     * @param record a output {@code ProducerRecord} for verification
     * @param expectedRecord a {@code ProducerRecord} for verification
     * @param <K> the key type
     * @param <V> the value type
     * @throws AssertionError if {@code ProducerRecord}'s key, value, or timestamp is not equal to
     * {@code expectedRecord}'s key, value, or timestamp
     */
    public static <K, V> void compareKeyValueTimestamp(final ProducerRecord<K, V> record,
                                                       final ProducerRecord<K, V> expectedRecord) throws AssertionError {
        Objects.requireNonNull(expectedRecord);
        compareKeyValueTimestamp(record, expectedRecord.key(), expectedRecord.value(), expectedRecord.timestamp());
    }

}
