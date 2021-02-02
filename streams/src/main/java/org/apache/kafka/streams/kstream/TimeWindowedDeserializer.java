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

import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.utils.Utils;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.state.internals.WindowKeySchema;

import java.util.Map;

/**
 *  The inner serde class can be specified by setting the property
 *  {@link StreamsConfig#DEFAULT_WINDOWED_KEY_SERDE_INNER_CLASS} or
 *  {@link StreamsConfig#DEFAULT_WINDOWED_VALUE_SERDE_INNER_CLASS}
 *  if the no-arg constructor is called and hence it is not passed during initialization.
 */
public class TimeWindowedDeserializer<T> implements Deserializer<Windowed<T>> {

    private Long windowSize;
    private boolean isChangelogTopic;

    private Deserializer<T> inner;

    // Default constructor needed by Kafka
    public TimeWindowedDeserializer() {
        this(null, null);
    }

    @Deprecated
    public TimeWindowedDeserializer(final Deserializer<T> inner) {
        this(inner, Long.MAX_VALUE);
    }

    public TimeWindowedDeserializer(final Deserializer<T> inner, final Long windowSize) {
        this.inner = inner;
        this.windowSize = windowSize;
        this.isChangelogTopic = false;
    }

    public Long getWindowSize() {
        return this.windowSize;
    }

    @SuppressWarnings("unchecked")
    @Override
    public void configure(final Map<String, ?> configs, final boolean isKey) {
        //check to see if the window size config is set and the window size is already set from the constructor
        final Long configWindowSize;
        if (configs.get(StreamsConfig.WINDOW_SIZE_MS_CONFIG) instanceof String) {
            configWindowSize = Long.parseLong((String) configs.get(StreamsConfig.WINDOW_SIZE_MS_CONFIG));
        } else {
            configWindowSize = (Long) configs.get(StreamsConfig.WINDOW_SIZE_MS_CONFIG);
        }
        if (windowSize != null && configWindowSize != null) {
            throw new IllegalArgumentException("Window size should not be set in both the time windowed deserializer constructor and the window.size.ms config");
        } else if (windowSize == null && configWindowSize == null) {
            throw new IllegalArgumentException("Window size needs to be set either through the time windowed deserializer " +
                "constructor or the window.size.ms config but not both");
        } else {
            windowSize = windowSize == null ? configWindowSize : windowSize;
        }
        if (inner == null) {
            final String propertyName = isKey ? StreamsConfig.DEFAULT_WINDOWED_KEY_SERDE_INNER_CLASS : StreamsConfig.DEFAULT_WINDOWED_VALUE_SERDE_INNER_CLASS;
            final String value = (String) configs.get(propertyName);
            try {
                inner = Serde.class.cast(Utils.newInstance(value, Serde.class)).deserializer();
                inner.configure(configs, isKey);
            } catch (final ClassNotFoundException e) {
                throw new ConfigException(propertyName, value, "Serde class " + value + " could not be found.");
            }
        }
    }

    @Override
    public Windowed<T> deserialize(final String topic, final byte[] data) {
        WindowedSerdes.verifyInnerDeserializerNotNull(inner, this);

        if (data == null || data.length == 0) {
            return null;
        }

        // toStoreKeyBinary was used to serialize the data.
        if (this.isChangelogTopic) {
            return WindowKeySchema.fromStoreKey(data, windowSize, inner, topic);
        }

        // toBinary was used to serialize the data
        return WindowKeySchema.from(data, windowSize, inner, topic);
    }

    @Override
    public void close() {
        if (inner != null) {
            inner.close();
        }
    }

    public void setIsChangelogTopic(final boolean isChangelogTopic) {
        this.isChangelogTopic = isChangelogTopic;
    }

    // Only for testing
    Deserializer<T> innerDeserializer() {
        return inner;
    }
}
