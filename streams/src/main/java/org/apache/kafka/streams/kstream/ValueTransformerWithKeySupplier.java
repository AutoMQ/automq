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
package org.apache.kafka.streams.kstream;

/**
 * @param <K>  key type
 * @param <V>  value type
 * @param <VR> transformed value type
 * @see ValueTransformer
 * @see ValueTransformerWithKey
 * @see KStream#transformValues(ValueTransformerSupplier, String...)
 * @see KStream#transformValues(ValueTransformerWithKeySupplier, String...)
 * @see Transformer
 * @see TransformerSupplier
 * @see KStream#transform(TransformerSupplier, String...)
 */
public interface ValueTransformerWithKeySupplier<K, V, VR> {
    ValueTransformerWithKey<K, V, VR> get();
}
