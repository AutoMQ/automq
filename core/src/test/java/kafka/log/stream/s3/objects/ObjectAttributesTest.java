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

package kafka.log.stream.s3.objects;

import com.automq.stream.s3.objects.ObjectAttributes;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class ObjectAttributesTest {

    @Test
    public void test() {
        ObjectAttributes attributes = ObjectAttributes.builder().type(ObjectAttributes.Type.Composite).bucket((short) 10).deepDelete().build();
        attributes = ObjectAttributes.from(attributes.attributes());
        assertEquals(ObjectAttributes.Type.Composite, attributes.type());
        assertEquals((short) 10, attributes.bucket());
        assertTrue(attributes.deepDelete());
    }

}
