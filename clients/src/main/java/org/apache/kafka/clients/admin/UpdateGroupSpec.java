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

package org.apache.kafka.clients.admin;

import java.util.Objects;

public class UpdateGroupSpec {
    private String linkId;
    private boolean promoted;
    private boolean verify = false; // 新增：是否验证 join attempts

    public UpdateGroupSpec linkId(String linkId) {
        this.linkId = linkId;
        return this;
    }

    public UpdateGroupSpec promoted(boolean promoted) {
        this.promoted = promoted;
        return this;
    }

    public UpdateGroupSpec verify(boolean verify) {
        this.verify = verify;
        return this;
    }

    public String linkId() {
        return linkId;
    }

    public boolean promoted() {
        return promoted;
    }

    public boolean verify() {
        return verify;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o)
            return true;
        if (o == null || getClass() != o.getClass())
            return false;
        UpdateGroupSpec spec = (UpdateGroupSpec) o;
        return promoted == spec.promoted && verify == spec.verify && Objects.equals(linkId, spec.linkId);
    }

    @Override
    public int hashCode() {
        return Objects.hash(linkId, promoted, verify);
    }

    @Override
    public String toString() {
        return "UpdateGroupsSpec{" +
            "linkId='" + linkId + '\'' +
            ", promoted=" + promoted +
            ", verify=" + verify +
            '}';
    }
}
