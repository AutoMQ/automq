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

package org.apache.kafka.common.protocol;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.databind.JsonNode;
import org.apache.kafka.common.protocol.types.RawTaggedField;
import org.apache.kafka.common.protocol.types.Struct;

import java.util.List;

/**
 * An object that can serialize itself.  The serialization protocol is versioned.
 * Messages also implement toString, equals, and hashCode.
 */
public interface Message {
    /**
     * Returns the lowest supported API key of this message, inclusive.
     */
    short lowestSupportedVersion();

    /**
     * Returns the highest supported API key of this message, inclusive.
     */
    short highestSupportedVersion();

    /**
     * Returns the number of bytes it would take to write out this message.
     *
     * @param cache         The serialization size cache to populate.
     * @param version       The version to use.
     *
     * @throws {@see org.apache.kafka.common.errors.UnsupportedVersionException}
     *                      If the specified version is too new to be supported
     *                      by this software.
     */
    int size(ObjectSerializationCache cache, short version);

    /**
     * Writes out this message to the given Writable.
     *
     * @param writable      The destination writable.
     * @param cache         The object serialization cache to use.  You must have
     *                      previously populated the size cache using #{Message#size()}.
     * @param version       The version to use.
     *
     * @throws {@see org.apache.kafka.common.errors.UnsupportedVersionException}
     *                      If the specified version is too new to be supported
     *                      by this software.
     */
    void write(Writable writable, ObjectSerializationCache cache, short version);

    /**
     * Reads this message from the given Readable.  This will overwrite all
     * relevant fields with information from the byte buffer.
     *
     * @param readable      The source readable.
     * @param version       The version to use.
     *
     * @throws {@see org.apache.kafka.common.errors.UnsupportedVersionException}
     *                      If the specified version is too new to be supported
     *                      by this software.
     */
    void read(Readable readable, short version);

    /**
     * Reads this message from a Struct object.  This will overwrite all
     * relevant fields with information from the Struct.
     *
     * @param struct        The source struct.
     * @param version       The version to use.
     *
     * @throws {@see org.apache.kafka.common.errors.UnsupportedVersionException}
     *                      If the specified struct can't be processed with the
     *                      specified message version.
     */
    void fromStruct(Struct struct, short version);

    /**
     * Writes out this message to a Struct.
     *
     * @param version       The version to use.
     *
     * @throws {@see org.apache.kafka.common.errors.UnsupportedVersionException}
     *                      If the specified version is too new to be supported
     *                      by this software.
     */
    Struct toStruct(short version);

    /**
     * Reads this message from a Jackson JsonNode object.  This will overwrite
     * all relevant fields with information from the Struct.
     *
     * For the most part, we expect every JSON object in the input to be the
     * correct type.  There is one exception: we will deserialize numbers
     * represented as strings.  If the numeric string begins with 0x, we will
     * treat the number as hexadecimal.
     *
     * Note that we expect to see NullNode objects created for null entries.
     * Therefore, please configure your Jackson ObjectMapper with
     * setSerializationInclusion({@link JsonInclude.Include#ALWAYS}).
     * Other settings may silently omit the nulls, which is not the
     * semantic that Kafka RPC uses.  (Including a field and setting it to
     * null is different than not including the field.)
     *
     * @param node          The source node.
     * @param version       The version to use.
     *
     * @throws {@see org.apache.kafka.common.errors.UnsupportedVersionException}
     *                      If the specified JSON can't be processed with the
     *                      specified message version.
     */
    void fromJson(JsonNode node, short version);

    /**
     * Convert this message to a JsonNode.
     *
     * Note that 64-bit numbers will be serialized as strings rather than as integers.
     * The reason is because JavaScript can't represent numbers above 2**52 accurately.
     * Therefore, for maximum interoperability, we represent these numbers as strings.
     *
     * @param version       The version to use.
     *
     * @throws {@see org.apache.kafka.common.errors.UnsupportedVersionException}
     *                      If the specified version is too new to be supported
     *                      by this software.
     */
    JsonNode toJson(short version);

    /**
     * Returns a list of tagged fields which this software can't understand.
     *
     * @return              The raw tagged fields.
     */
    List<RawTaggedField> unknownTaggedFields();
}
