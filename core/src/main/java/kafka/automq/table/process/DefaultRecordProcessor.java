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

import kafka.automq.table.process.exception.ConverterException;
import kafka.automq.table.process.exception.InvalidDataException;
import kafka.automq.table.process.exception.RecordProcessorException;
import kafka.automq.table.process.exception.SchemaRegistrySystemException;
import kafka.automq.table.process.exception.TransformException;

import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.record.Record;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.jetbrains.annotations.NotNull;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

import io.confluent.kafka.schemaregistry.client.rest.exceptions.RestClientException;

import static java.net.HttpURLConnection.HTTP_NOT_FOUND;
import static kafka.automq.table.process.RecordAssembler.KAFKA_VALUE_FIELD;

/**
 * Default implementation of RecordProcessor using a two-stage processing pipeline.
 *
 * @see RecordProcessor
 * @see Converter
 * @see Transform
 */
public class DefaultRecordProcessor implements RecordProcessor {
    private static final Schema HEADER_SCHEMA = Schema.createMap(Schema.create(Schema.Type.BYTES));
    private static final String HEADER_SCHEMA_IDENTITY = String.valueOf(HEADER_SCHEMA.hashCode());
    private final String topicName;
    private final Converter keyConverter;
    private final Converter valueConverter;
    private final List<Transform> transformChain;

    public DefaultRecordProcessor(String topicName, Converter keyConverter, Converter valueConverter) {
        this.transformChain = new ArrayList<>();
        this.topicName = topicName;
        this.keyConverter = keyConverter;
        this.valueConverter = valueConverter;
    }

    public DefaultRecordProcessor(String topicName, Converter keyConverter, Converter valueConverter, List<Transform> transforms) {
        this.transformChain = transforms;
        this.topicName = topicName;
        this.keyConverter = keyConverter;
        this.valueConverter = valueConverter;
    }

    @Override
    public ProcessingResult process(int partition, Record kafkaRecord) {
        try {
            Objects.requireNonNull(kafkaRecord, "Kafka record cannot be null");

            ConversionResult headerResult = processHeaders(kafkaRecord);
            ConversionResult keyResult = keyConverter.convert(topicName, kafkaRecord.key());
            ConversionResult valueResult = valueConverter.convert(topicName, kafkaRecord.value());

            GenericRecord baseRecord = wrapValue(valueResult);
            GenericRecord transformedRecord = applyTransformChain(baseRecord, partition, kafkaRecord);
            GenericRecord finalRecord = new RecordAssembler(transformedRecord)
                .withHeader(headerResult)
                .withKey(keyResult)
                .withMetadata(partition, kafkaRecord.offset(), kafkaRecord.timestamp())
                .assemble();
            Schema finalSchema = finalRecord.getSchema();
            String schemaIdentity = generateCompositeSchemaIdentity(headerResult, keyResult, valueResult, transformChain);

            return new ProcessingResult(finalRecord, finalSchema, schemaIdentity);
        } catch (ConverterException e) {
            return getProcessingResult(kafkaRecord, "Convert operation failed for record: %s", DataError.ErrorType.CONVERT_ERROR, e);
        } catch (TransformException e) {
            return getProcessingResult(kafkaRecord, "Transform operation failed for record: %s", DataError.ErrorType.TRANSFORMATION_ERROR, e);
        } catch (InvalidDataException e) {
            return getProcessingResult(kafkaRecord, "Transform operation failed for record: %s", DataError.ErrorType.DATA_ERROR, e);
        } catch (Exception e) {
            if (e.getCause() instanceof RestClientException) {
                RestClientException exception = (RestClientException) e.getCause();
                // io.confluent.kafka.serializers.AbstractKafkaSchemaSerDe#toKafkaException
                if (isSchemaOrSubjectNotFoundException(exception)) { // not found
                    return getProcessingResult(kafkaRecord, "Schema or subject not found for record: %s", DataError.ErrorType.CONVERT_ERROR, exception);
                }
                throw SchemaRegistrySystemException.fromStatusCode(exception, buildRecordContext(kafkaRecord));
            }
            return getProcessingResult(kafkaRecord, "Unexpected error processing record: %s", DataError.ErrorType.UNKNOW_ERROR, e);
        }
    }

    // io.confluent.kafka.schemaregistry.client.CachedSchemaRegistryClient#isSchemaOrSubjectNotFoundException
    private boolean isSchemaOrSubjectNotFoundException(RestClientException rce) {
        return rce.getStatus() == HTTP_NOT_FOUND
            && (rce.getErrorCode() == 40403 // SCHEMA_NOT_FOUND_ERROR_CODE
            || rce.getErrorCode() == 40401); // SUBJECT_NOT_FOUND_ERROR_CODE
    }


    @NotNull
    private ProcessingResult getProcessingResult(Record kafkaRecord, String format, DataError.ErrorType unknow, Exception e) {
        String recordContext = buildRecordContext(kafkaRecord);
        String errorMsg = String.format(format, recordContext);
        DataError error = new DataError(unknow, errorMsg + ": " + e.getMessage(), e);
        return new ProcessingResult(error);
    }

    private ConversionResult processHeaders(Record kafkaRecord) throws ConverterException {
        try {
            Map<String, ByteBuffer> headers = new HashMap<>();
            Header[] recordHeaders = kafkaRecord.headers();
            if (recordHeaders != null) {
                for (Header header : recordHeaders) {
                    ByteBuffer value = header.value() != null ?
                        ByteBuffer.wrap(header.value()) : null;
                    headers.put(header.key(), value);
                }
            }
            return new ConversionResult(headers, HEADER_SCHEMA, HEADER_SCHEMA_IDENTITY);
        } catch (Exception e) {
            throw new ConverterException("Failed to process headers", e);
        }
    }

    private GenericRecord wrapValue(ConversionResult valueResult) {
        Schema.Field valueField = new Schema.Field(KAFKA_VALUE_FIELD, valueResult.getSchema(), null, null);
        Object valueContent = valueResult.getValue();

        Schema recordSchema = Schema.createRecord("KafkaValueWrapper", null, "kafka.automq.table.process", false);
        recordSchema.setFields(Collections.singletonList(valueField));

        GenericRecord baseRecord = new GenericData.Record(recordSchema);
        baseRecord.put(KAFKA_VALUE_FIELD, valueContent);

        return baseRecord;
    }

    private GenericRecord applyTransformChain(GenericRecord baseRecord, int partition, Record kafkaRecord) throws TransformException {
        if (transformChain.isEmpty()) {
            return baseRecord;
        }

        GenericRecord currentRecord = baseRecord;
        TransformContext context = new TransformContext(kafkaRecord, topicName, partition);

        for (Transform transform : transformChain) {
            currentRecord = transform.apply(currentRecord, context);
            if (currentRecord == null) {
                throw new TransformException("Transform " + transform.getName() + " returned null record");
            }
        }
        return currentRecord;
    }


    private String generateCompositeSchemaIdentity(
        ConversionResult headerResult,
        ConversionResult keyResult,
        ConversionResult valueResult,
        List<Transform> transforms) {

        // Extract schema identities with null safety
        String headerIdentity = headerResult.getSchemaIdentity();
        String keyIdentity = keyResult.getSchemaIdentity();
        String valueIdentity = valueResult.getSchemaIdentity();

        // Generate transform chain identity
        String transformIdentity = transforms.isEmpty() ?
            "noTransform" :
            transforms.stream()
                .map(Transform::getName)
                .collect(java.util.stream.Collectors.joining("->"));

        // Join all parts with the identity separator
        return String.join("|", headerIdentity, keyIdentity, valueIdentity, transformIdentity);
    }

    @Override
    public void configure(Map<String, ?> configs) throws RecordProcessorException {
        // ignore
    }

    private String buildRecordContext(org.apache.kafka.common.record.Record kafkaRecord) {
        return String.format("topic=%s, key=%s, offset=%d, timestamp=%d",
                           topicName,
                           kafkaRecord.key(),
                           kafkaRecord.offset(),
                           kafkaRecord.timestamp());
    }

    public String getTopicName() {
        return topicName;
    }

}
