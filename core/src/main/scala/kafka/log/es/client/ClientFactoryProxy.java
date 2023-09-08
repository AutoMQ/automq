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

package kafka.log.es.client;

import kafka.log.es.api.Client;

import java.lang.reflect.Method;

public class ClientFactoryProxy {
    private static final String PROTOCOL_SEPARATOR = ":";
    private static final String FACTORY_CLASS_FORMAT = "kafka.log.es.client.%s.ClientFactory";

    public static Client get(Context context) {
        String endpoint = context.config.elasticStreamEndpoint();
        String protocol = endpoint.split(PROTOCOL_SEPARATOR)[0];
        String className = String.format(FACTORY_CLASS_FORMAT, protocol);
        try {
            Class<?> clazz = Class.forName(className);
            Method method = clazz.getDeclaredMethod("get", Context.class);
            return (Client) method.invoke(null, context);
        } catch (Throwable e) {
            throw new RuntimeException(e);
        }
    }

}
