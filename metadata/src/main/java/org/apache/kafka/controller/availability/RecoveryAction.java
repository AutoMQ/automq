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

package org.apache.kafka.controller.availability;

import org.apache.kafka.common.Uuid;

import com.fasterxml.jackson.annotation.JsonGetter;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonSetter;

import java.util.Objects;

@JsonIgnoreProperties(ignoreUnknown = true)
public class RecoveryAction {
    private int schemaVersion = AvailabilityConstants.SCHEMA_VERSION;
    private Uuid actionUuid;
    private AvailabilityActionType actionType;
    private AvailabilityTarget target;
    private int executorBrokerId = -1;
    private long createdAtMs;
    private long deadlineMs;
    private String reason;
    private boolean dryRun;

    public RecoveryAction() {
    }

    public RecoveryAction(Uuid actionUuid, AvailabilityActionType actionType, AvailabilityTarget target,
                          int executorBrokerId, long createdAtMs, long deadlineMs, String reason, boolean dryRun) {
        this.actionUuid = actionUuid;
        this.actionType = actionType;
        this.target = target;
        this.executorBrokerId = executorBrokerId;
        this.createdAtMs = createdAtMs;
        this.deadlineMs = deadlineMs;
        this.reason = reason;
        this.dryRun = dryRun;
    }

    public int getSchemaVersion() {
        return schemaVersion;
    }

    public void setSchemaVersion(int schemaVersion) {
        this.schemaVersion = schemaVersion;
    }

    public Uuid getActionUuid() {
        return actionUuid;
    }

    public void setActionUuid(Uuid actionUuid) {
        this.actionUuid = actionUuid;
    }

    @JsonGetter("actionUuid")
    public String getActionUuidValue() {
        return actionUuid == null ? null : actionUuid.toString();
    }

    @JsonSetter("actionUuid")
    public void setActionUuidValue(String actionUuid) {
        this.actionUuid = actionUuid == null ? null : Uuid.fromString(actionUuid);
    }

    public AvailabilityActionType getActionType() {
        return actionType;
    }

    public void setActionType(AvailabilityActionType actionType) {
        this.actionType = actionType;
    }

    public AvailabilityTarget getTarget() {
        return target;
    }

    public void setTarget(AvailabilityTarget target) {
        this.target = target;
    }

    public int getExecutorBrokerId() {
        return executorBrokerId;
    }

    public void setExecutorBrokerId(int executorBrokerId) {
        this.executorBrokerId = executorBrokerId;
    }

    public long getCreatedAtMs() {
        return createdAtMs;
    }

    public void setCreatedAtMs(long createdAtMs) {
        this.createdAtMs = createdAtMs;
    }

    public long getDeadlineMs() {
        return deadlineMs;
    }

    public void setDeadlineMs(long deadlineMs) {
        this.deadlineMs = deadlineMs;
    }

    public String getReason() {
        return reason;
    }

    public void setReason(String reason) {
        this.reason = reason;
    }

    public boolean isDryRun() {
        return dryRun;
    }

    public void setDryRun(boolean dryRun) {
        this.dryRun = dryRun;
    }

    @Override
    public boolean equals(Object o) {
        if (!(o instanceof RecoveryAction)) {
            return false;
        }
        RecoveryAction that = (RecoveryAction) o;
        return schemaVersion == that.schemaVersion &&
            executorBrokerId == that.executorBrokerId &&
            createdAtMs == that.createdAtMs &&
            deadlineMs == that.deadlineMs &&
            dryRun == that.dryRun &&
            Objects.equals(actionUuid, that.actionUuid) &&
            actionType == that.actionType &&
            Objects.equals(target, that.target) &&
            Objects.equals(reason, that.reason);
    }

    @Override
    public int hashCode() {
        return Objects.hash(schemaVersion, actionUuid, actionType, target, executorBrokerId, createdAtMs, deadlineMs,
            reason, dryRun);
    }
}
