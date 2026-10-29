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

package kafka.automq.table;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import org.apache.parquet.hadoop.metadata.FileMetaData;
import org.apache.parquet.hadoop.metadata.ParquetMetadata;
import org.apache.parquet.schema.MessageTypeParser;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.Collections;
import java.util.ServiceLoader;

import shaded.parquet.com.fasterxml.jackson.core.JsonFactory;
import shaded.parquet.com.fasterxml.jackson.core.ObjectCodec;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** Checks Parquet compatibility and service discovery for the rebuilt shaded Jackson runtime. */
@Tag("S3Unit")
public class ParquetJacksonTest {

    /** Verifies that Parquet's existing metadata serializer can link to the rebuilt Jackson classes. */
    @Test
    public void testParquetMetadataSerialization() throws IOException {
        FileMetaData fileMetaData = new FileMetaData(
            MessageTypeParser.parseMessageType("message example { required binary name (UTF8); }"),
            Collections.singletonMap("source", "table-topic"), "jackson-upgrade-test",
            FileMetaData.EncryptionType.UNENCRYPTED, null);
        ParquetMetadata metadata = new ParquetMetadata(fileMetaData, Collections.emptyList());

        JsonNode json = new ObjectMapper().readTree(ParquetMetadata.toJSON(metadata));
        assertEquals("jackson-upgrade-test", json.path("fileMetaData").path("createdBy").asText());
        assertEquals("table-topic", json.path("fileMetaData").path("keyValueMetaData").path("source").asText());
        assertTrue(json.path("blocks").isArray());
        assertEquals(0, json.path("blocks").size());
    }

    /** Verifies version alignment, absence of duplicate shaded classes, and relocated service providers. */
    @Test
    public void testShadedJacksonProviders() throws IOException {
        ClassLoader loader = ParquetMetadata.class.getClassLoader();
        assertEquals(1, Collections.list(loader.getResources(
            "shaded/parquet/com/fasterxml/jackson/databind/ObjectMapper.class")).size());
        ObjectCodec codec = ServiceLoader.load(ObjectCodec.class, loader).findFirst().orElseThrow();
        assertEquals(new ObjectMapper().version().toString(), codec.version().toString());
        JsonFactory factory = ServiceLoader.load(JsonFactory.class, loader).findFirst().orElseThrow();
        assertEquals(new com.fasterxml.jackson.core.JsonFactory().version().toString(), factory.version().toString());
    }
}
