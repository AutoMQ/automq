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
import kafka.automq.table.process.exception.RecordProcessorException;
import kafka.automq.table.process.exception.TransformException;

import org.apache.avro.generic.GenericRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * Default implementation of RecordProcessor using a two-stage processing pipeline.
 *
 * @see RecordProcessor
 * @see Converter
 * @see Transform
 */
public class DefaultRecordProcessor implements RecordProcessor {

    private static final Logger log = LoggerFactory.getLogger(DefaultRecordProcessor.class);

    // Core components
    private String topicName;
    private Converter converter;
    private List<Transform> transformChain;

    public DefaultRecordProcessor(String topicName, Converter converter) {
        this.transformChain = new ArrayList<>();
        this.topicName = topicName;
        this.converter = converter;
    }

    public DefaultRecordProcessor(String topicName, Converter converter, List<Transform> transforms) {
        this.transformChain = transforms;
        this.topicName = topicName;
        this.converter = converter;
    }

    @Override
    public ProcessingResult process(org.apache.kafka.common.record.Record kafkaRecord) {
        try {
            Objects.requireNonNull(kafkaRecord, "Kafka record cannot be null");
            if (converter == null) {
                throw new RecordProcessorException("Converter not configured, this is a fatal error");
            }

            ConversionResult conversionResult = converter.convert(topicName, kafkaRecord);

            GenericRecord transformedRecord = applyTransformChain(conversionResult);

            // Stage 3: Final Avro Record Creation
            return createFinalAvroRecord(conversionResult, transformedRecord);
        } catch (ConverterException e) {
            String recordContext = buildRecordContext(kafkaRecord);
            String errorMsg = String.format("Convert operation failed for record: %s", recordContext);
            DataError error = new DataError(DataError.ErrorType.DATA_ERROR, errorMsg + ": " + e.getMessage(), e);
            return new ProcessingResult(error);

        } catch (TransformException e) {
            String recordContext = buildRecordContext(kafkaRecord);
            String errorMsg = String.format("Transform operation failed for record: %s", recordContext);
            DataError error = new DataError(DataError.ErrorType.TRANSFORMATION_ERROR, errorMsg + ": " + e.getMessage(), e);
            return new ProcessingResult(error);

        } catch (Exception e) {
            String recordContext = buildRecordContext(kafkaRecord);
            String errorMsg = String.format("Unexpected error processing record: %s", recordContext);
            DataError error = new DataError(DataError.ErrorType.UNKNOW, errorMsg + ": " + e.getMessage(), e);
            return new ProcessingResult(error);
        }
    }

    private GenericRecord applyTransformChain(ConversionResult conversionResult) throws TransformException {
        GenericRecord currentRecord = conversionResult.getValueRecord();
        TransformContext context = new TransformContext(conversionResult.getKafkaRecord(), topicName);

        for (Transform transform : transformChain) {
            currentRecord = transform.apply(currentRecord, context);

            if (currentRecord == null) {
                throw new TransformException("Transform " + transform.getName() + " returned null record");
            }
        }

        return currentRecord;
    }


    private ProcessingResult createFinalAvroRecord(ConversionResult conversionResult, GenericRecord transformedRecord) {
        // Use the schema identity from the conversion result if available,
        // otherwise generate one from the final schema
        String schemaIdentity = conversionResult.getSchemaIdentity();
        if (schemaIdentity == null) {
            // Fallback: generate schema identity at Process layer if Converter didn't provide one
            schemaIdentity = generateSchemaIdentity(transformedRecord.getSchema());
        }
        return new ProcessingResult(transformedRecord, transformedRecord.getSchema(), schemaIdentity);
    }

    @Override
    public void configure(Map<String, ?> configs) throws RecordProcessorException {
        // ignore
    }

    private String generateSchemaIdentity(org.apache.avro.Schema schema) {
        return String.valueOf(schema.toString().hashCode());
    }

    /**
     * Builds a descriptive context string for a Kafka record to include in error messages.
     */
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

    public List<Transform> getTransformChain() {
        return Collections.unmodifiableList(transformChain);
    }

}
