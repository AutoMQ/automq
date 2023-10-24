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

package kafka.log.streamaspect.client.s3;

import com.automq.stream.s3.metrics.S3StreamMetricsRegistry;
import org.apache.kafka.server.metrics.s3stream.KafkaS3StreamMetricsGroup;
import com.automq.stream.api.Client;
import kafka.log.stream.s3.DefaultS3Client;
import kafka.log.streamaspect.AlwaysSuccessClient;
import kafka.log.streamaspect.client.Context;

public class ClientFactory {
    public static Client get(Context context) {
        S3StreamMetricsRegistry.setMetricsGroup(new KafkaS3StreamMetricsGroup());
        DefaultS3Client client = new DefaultS3Client(context.brokerServer, context.config);
        return new AlwaysSuccessClient(client);
    }
}
