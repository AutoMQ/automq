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
package org.apache.kafka.connect.runtime.isolation;

import com.fasterxml.jackson.annotation.JsonProperty;
import org.apache.maven.artifact.versioning.DefaultArtifactVersion;

import java.util.Objects;

public class PluginDesc<T> implements Comparable<PluginDesc<T>> {
    private final Class<? extends T> klass;
    private final String name;
    private final String version;
    private final DefaultArtifactVersion encodedVersion;
    private final PluginType type;
    private final String typeName;
    private final String location;

    public PluginDesc(Class<? extends T> klass, String version, ClassLoader loader) {
        this.klass = klass;
        this.name = klass.getName();
        this.version = version != null ? version : "null";
        this.encodedVersion = new DefaultArtifactVersion(this.version);
        this.type = PluginType.from(klass);
        this.typeName = type.toString();
        this.location = loader instanceof PluginClassLoader
                ? ((PluginClassLoader) loader).location()
                : "classpath";
    }

    @Override
    public String toString() {
        return "PluginDesc{" +
                "klass=" + klass +
                ", name='" + name + '\'' +
                ", version='" + version + '\'' +
                ", encodedVersion=" + encodedVersion +
                ", type=" + type +
                ", typeName='" + typeName + '\'' +
                ", location='" + location + '\'' +
                '}';
    }

    public Class<? extends T> pluginClass() {
        return klass;
    }

    @JsonProperty("class")
    public String className() {
        return name;
    }

    @JsonProperty("version")
    public String version() {
        return version;
    }

    public PluginType type() {
        return type;
    }

    @JsonProperty("type")
    public String typeName() {
        return typeName;
    }

    @JsonProperty("location")
    public String location() {
        return location;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof PluginDesc)) {
            return false;
        }
        PluginDesc<?> that = (PluginDesc<?>) o;
        return Objects.equals(klass, that.klass) &&
                Objects.equals(version, that.version) &&
                type == that.type;
    }

    @Override
    public int hashCode() {
        return Objects.hash(klass, version, type);
    }

    @Override
    public int compareTo(PluginDesc<T> other) {
        int nameComp = name.compareTo(other.name);
        return nameComp != 0 ? nameComp : encodedVersion.compareTo(other.encodedVersion);
    }
}
