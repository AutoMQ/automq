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

package kafka.automq.table;

import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.Files;
import org.apache.iceberg.Schema;
import org.apache.iceberg.data.GenericRecord;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.data.parquet.GenericParquetReaders;
import org.apache.iceberg.data.parquet.GenericParquetWriter;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.io.FileAppender;
import org.apache.iceberg.parquet.Parquet;
import org.apache.iceberg.types.Types;
import org.apache.parquet.hadoop.ParquetFileReader;
import org.apache.parquet.hadoop.metadata.ParquetMetadata;
import org.apache.parquet.hadoop.util.HadoopInputFile;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;

import shaded.parquet.com.fasterxml.jackson.databind.JsonNode;
import shaded.parquet.com.fasterxml.jackson.databind.ObjectMapper;

import static org.junit.jupiter.api.Assertions.assertEquals;

@Tag("S3Unit")
class ParquetCompatibilityTest {
    /**
     * Existing Iceberg and Parquet readers, writers, and footer JSON must remain compatible
     * when the independently shaded Jackson dependency is upgraded.
     */
    @Test
    void testParquetRecordsAndShadedMetadataJson(@TempDir Path directory) throws IOException {
        Schema schema = new Schema(
            Types.NestedField.required(1, "id", Types.LongType.get()),
            Types.NestedField.optional(2, "message", Types.StringType.get()));
        GenericRecord first = GenericRecord.create(schema);
        first.setField("id", 1L);
        first.setField("message", "quoted \"value\"\nwith a newline");
        GenericRecord second = GenericRecord.create(schema);
        second.setField("id", 2L);
        List<Record> expected = List.of(first, second);
        Path file = directory.resolve("records.parquet");
        String metadataValue = "escaped \"metadata\"\nvalue";

        try (FileAppender<Record> writer = Parquet.write(Files.localOutput(file.toFile()))
            .schema(schema)
            .meta("compatibility", metadataValue)
            .createWriterFunc(GenericParquetWriter::buildWriter)
            .build()) {
            writer.addAll(expected);
        }

        List<Record> actual = new ArrayList<>();
        try (CloseableIterable<Record> reader = Parquet.read(Files.localInput(file.toFile()))
            .project(schema)
            .createReaderFunc(parquetSchema -> GenericParquetReaders.buildReader(schema, parquetSchema))
            .build()) {
            reader.forEach(actual::add);
        }
        assertEquals(expected, actual);

        HadoopInputFile input = HadoopInputFile.fromPath(
            new org.apache.hadoop.fs.Path(file.toUri()), new Configuration(false));
        try (ParquetFileReader reader = ParquetFileReader.open(input)) {
            ParquetMetadata footer = reader.getFooter();
            ObjectMapper mapper = new ObjectMapper();
            JsonNode json = mapper.readTree(ParquetMetadata.toJSON(footer));
            assertEquals(metadataValue, json.path("fileMetaData").path("keyValueMetaData")
                .path("compatibility").asText());
            assertEquals(2, json.path("blocks").get(0).path("rowCount").asLong());
            assertEquals(json, mapper.readTree(ParquetMetadata.toPrettyJSON(footer)));
        }
    }
}
