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

package kafka.log.stream.s3;

import com.automq.stream.s3.Config;
import com.automq.stream.s3.operator.AwsObjectStorage;
import com.automq.stream.s3.operator.BucketURI;
import com.automq.stream.s3.operator.ObjectStorageFactory;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.MockedConstruction;

import java.lang.reflect.Field;
import java.time.Duration;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.mockito.Mockito.CALLS_REAL_METHODS;
import static org.mockito.Mockito.RETURNS_SELF;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockConstruction;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.withSettings;

@Tag("S3Unit")
class DefaultS3ClientBackgroundStorageTest {
    /** Background timeout overrides and isolation must not mutate shared main-storage bucket options. */
    @Test
    @SuppressWarnings("unchecked")
    void testBackgroundOverridesAreIsolated() throws Exception {
        BucketURI original = BucketURI.parse("0@s3://bucket?apiCallAttemptTimeoutMs=1234&apiCallTimeoutMs=5678&numRetries=2");
        DefaultS3Client client = mock(DefaultS3Client.class, CALLS_REAL_METHODS);
        Field config = DefaultS3Client.class.getDeclaredField("config");
        config.setAccessible(true);
        config.set(client, new Config().dataBuckets(List.of(original)));
        try (MockedConstruction<ObjectStorageFactory.Builder> builders = mockConstruction(ObjectStorageFactory.Builder.class,
            withSettings().defaultAnswer(RETURNS_SELF))) {
            client.newBackgroundObjectStorage();
            ObjectStorageFactory.Builder builder = builders.constructed().get(0);
            ArgumentCaptor<List<BucketURI>> buckets = ArgumentCaptor.forClass(List.class);
            verify(builder).buckets(buckets.capture());
            verify(builder).readWriteIsolate(true);
            BucketURI background = buckets.getValue().get(0);
            assertNotSame(original, background);
            AwsObjectStorage.Configuration options = AwsObjectStorage.Configuration.fromBucketURI(background);
            assertEquals(Duration.ofSeconds(60), options.apiCallAttemptTimeout());
            assertEquals(Duration.ofSeconds(90), options.apiCallTimeout());
            assertEquals(0, options.numRetries());
            assertEquals("1234", original.extensionString(BucketURI.API_CALL_ATTEMPT_TIMEOUT_KEY));
            assertEquals("5678", original.extensionString(BucketURI.API_CALL_TIMEOUT_KEY));
            assertEquals("2", original.extensionString(BucketURI.NUM_RETRIES_KEY));
        }
    }
}
