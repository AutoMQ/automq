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

package kafka.log.es.client.es;

import com.automq.elasticstream.client.DefaultClientBuilder;
import com.automq.elasticstream.client.api.Client;
import kafka.log.es.AlwaysSuccessClient;
import kafka.log.es.client.Context;

public class ClientFactory {
    private static final String ES_ENDPOINT_PREFIX = "es://";

    public static Client get(Context context) {
        String endpoint = context.config.elasticStreamEndpoint();
        String kvEndpoint = context.config.elasticStreamKvEndpoint();
        if (!kvEndpoint.startsWith(ES_ENDPOINT_PREFIX)) {
            throw new IllegalArgumentException("Elastic stream endpoint and kvEndpoint must be the same protocol: " + endpoint + " " + kvEndpoint);
        }
        Client client = new DefaultClientBuilder()
                .endpoint(endpoint.substring(ES_ENDPOINT_PREFIX.length()))
                .kvEndpoint(kvEndpoint.substring(ES_ENDPOINT_PREFIX.length()))
                .build();
        return new AlwaysSuccessClient(client);
    }

}
