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

package test.plugins;

import java.util.Map;
import java.util.HashMap;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaAndValue;
import org.apache.kafka.connect.storage.Converter;
import org.apache.kafka.connect.runtime.isolation.SamplingTestPlugin;

/**
 * Samples data about its initialization environment for later analysis
 * Samples are shared between instances of the same class in a static variable
 */
public class AliasedStaticField extends SamplingTestPlugin implements Converter {

  private static final Map<String, SamplingTestPlugin> SAMPLES;
  private static final ClassLoader STATIC_CLASS_LOADER;
  private final ClassLoader classloader;

  static {
    SAMPLES = new HashMap<>();
    STATIC_CLASS_LOADER = Thread.currentThread().getContextClassLoader();
  }

  {
    classloader = Thread.currentThread().getContextClassLoader();
  }

  @Override
  public void configure(final Map<String, ?> configs, final boolean isKey) {

  }

  @Override
  public byte[] fromConnectData(final String topic, final Schema schema, final Object value) {
    return new byte[0];
  }

  @Override
  public SchemaAndValue toConnectData(final String topic, final byte[] value) {
    return null;
  }

  @Override
  public ClassLoader staticClassloader() {
    return STATIC_CLASS_LOADER;
  }

  @Override
  public ClassLoader classloader() {
    return classloader;
  }

  @Override
  public Map<String, SamplingTestPlugin> otherSamples() {
      return SAMPLES;
  }
}
