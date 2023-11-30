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

package org.apache.kafka.metadata.placement;

import org.apache.kafka.common.DirectoryId;
import org.apache.kafka.common.Uuid;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Objects;

/**
 * The partition assignment.
 *
 * The assignment is represented as a list of integers and {@link Uuid}s
 * where each integer is the replica ID, and each Uuid is the ID of the
 * directory hosting the replica in the broker.
 * This class is immutable. It's internal state does not change.
 */
public class PartitionAssignment {

    private final List<Integer> replicas;
    private final List<Uuid> directories;

    // TODO remove -- just here for testing
    public PartitionAssignment(List<Integer> replicas) {
        this(replicas, brokerId -> DirectoryId.UNASSIGNED);
    }

    public PartitionAssignment(List<Integer> replicas, DefaultDirProvider defaultDirProvider) {
        this.replicas = Collections.unmodifiableList(new ArrayList<>(replicas));
        Uuid[] directories = new Uuid[replicas.size()];
        for (int i = 0; i < directories.length; i++) {
            directories[i] = defaultDirProvider.defaultDir(replicas.get(i));
        }
        this.directories = Collections.unmodifiableList(Arrays.asList(directories));
    }

    /**
     * @return The partition assignment that consists of a list of replica IDs.
     */
    public List<Integer> replicas() {
        return replicas;
    }

    public List<Uuid> directories() {
        return directories;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        PartitionAssignment that = (PartitionAssignment) o;
        return Objects.equals(replicas, that.replicas) && Objects.equals(directories, that.directories);
    }

    @Override
    public int hashCode() {
        return Objects.hash(replicas, directories);
    }

    @Override
    public String toString() {
        return "PartitionAssignment" +
                "(replicas=" + replicas +
                ", directories=" + directories +
                ")";
    }
}
