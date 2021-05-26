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
package org.apache.kafka.streams.processor;

import org.apache.kafka.streams.errors.TaskIdFormatException;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Objects;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.kafka.streams.processor.internals.assignment.ConsumerProtocolUtils.readTaskIdFrom;
import static org.apache.kafka.streams.processor.internals.assignment.ConsumerProtocolUtils.writeTaskIdTo;

/**
 * The task ID representation composed as subtopology (aka topicGroupId) plus the assigned partition ID.
 */
public class TaskId implements Comparable<TaskId> {

    private static final Logger LOG = LoggerFactory.getLogger(TaskId.class);

    /** The ID of the subtopology, aka topicGroupId. */
    @Deprecated
    public final int topicGroupId;
    /** The ID of the partition. */
    @Deprecated
    public final int partition;

    /** The namedTopology that this task belongs to, or null if it does not belong to one */
    private final String namedTopology;

    public TaskId(final int topicGroupId, final int partition) {
        this(topicGroupId, partition, null);
    }

    public TaskId(final int topicGroupId, final int partition, final String namedTopology) {
        this.topicGroupId = topicGroupId;
        this.partition = partition;
        if (namedTopology != null && namedTopology.length() == 0) {
            LOG.warn("Empty string passed in for task's namedTopology, since NamedTopology name cannot be empty, we "
                         + "assume this task does not belong to a NamedTopology and downgrade this to null");
            this.namedTopology = null;
        } else {
            this.namedTopology = namedTopology;
        }
    }

    public int subtopology() {
        return topicGroupId;
    }

    public int partition() {
        return partition;
    }

    /**
     * Experimental feature -- will return null
     */
    public String namedTopology() {
        return namedTopology;
    }

    @Override
    public String toString() {
        return namedTopology != null ? namedTopology + "_" + topicGroupId + "_" + partition : topicGroupId + "_" + partition;
    }

    /**
     * @throws TaskIdFormatException if the taskIdStr is not a valid {@link TaskId}
     */
    public static TaskId parse(final String taskIdStr) {
        final int firstIndex = taskIdStr.indexOf('_');
        final int secondIndex = taskIdStr.indexOf('_', firstIndex + 1);
        if (firstIndex <= 0 || firstIndex + 1 >= taskIdStr.length()) {
            throw new TaskIdFormatException(taskIdStr);
        }

        try {
            // If only one copy of '_' exists, there is no named topology in the string
            if (secondIndex < 0) {
                final int topicGroupId = Integer.parseInt(taskIdStr.substring(0, firstIndex));
                final int partition = Integer.parseInt(taskIdStr.substring(firstIndex + 1));

                return new TaskId(topicGroupId, partition);
            } else {
                final String namedTopology = taskIdStr.substring(0, firstIndex);
                final int topicGroupId = Integer.parseInt(taskIdStr.substring(firstIndex + 1, secondIndex));
                final int partition = Integer.parseInt(taskIdStr.substring(secondIndex + 1));

                return new TaskId(topicGroupId, partition, namedTopology);
            }
        } catch (final Exception e) {
            throw new TaskIdFormatException(taskIdStr);
        }
    }

    /**
     * @throws IOException if cannot write to output stream
     * @deprecated since 3.0, for internal use, will be removed
     */
    @Deprecated
    public void writeTo(final DataOutputStream out, final int version) throws IOException {
        writeTaskIdTo(this, out, version);
    }

    /**
     * @throws IOException if cannot read from input stream
     * @deprecated since 3.0, for internal use, will be removed
     */
    @Deprecated
    public static TaskId readFrom(final DataInputStream in, final int version) throws IOException {
        return readTaskIdFrom(in, version);
    }

    /**
     * @deprecated since 3.0, for internal use, will be removed
     */
    @Deprecated
    public void writeTo(final ByteBuffer buf, final int version) {
        writeTaskIdTo(this, buf, version);
    }

    /**
     * @deprecated since 3.0, for internal use, will be removed
     */
    @Deprecated
    public static TaskId readFrom(final ByteBuffer buf, final int version) {
        return readTaskIdFrom(buf, version);
    }

    @Override
    public boolean equals(final Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        final TaskId taskId = (TaskId) o;

        if (topicGroupId != taskId.topicGroupId || partition != taskId.partition) {
            return false;
        }

        if (namedTopology != null && taskId.namedTopology != null) {
            return namedTopology.equals(taskId.namedTopology);
        } else {
            return namedTopology == null && taskId.namedTopology == null;
        }
    }

    @Override
    public int hashCode() {
        return Objects.hash(topicGroupId, partition, namedTopology);
    }

    @Override
    public int compareTo(final TaskId other) {
        if (namedTopology != null && other.namedTopology != null) {
            final int comparingNamedTopologies = namedTopology.compareTo(other.namedTopology);
            if (comparingNamedTopologies != 0) {
                return comparingNamedTopologies;
            }
        } else if (namedTopology != null || other.namedTopology != null) {
            LOG.error("Tried to compare this = {} with other = {}, but only one had a valid named topology", this, other);
            throw new IllegalStateException("Can't compare a TaskId with a namedTopology to one without");
        }
        final int comparingTopicGroupId = Integer.compare(this.topicGroupId, other.topicGroupId);
        return comparingTopicGroupId != 0 ? comparingTopicGroupId : Integer.compare(this.partition, other.partition);
    }
}
