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
public class ActionResponse {
    private int schemaVersion = AvailabilityConstants.SCHEMA_VERSION;
    private Uuid actionUuid;
    private AvailabilityActionType actionType;
    private AvailabilityTarget target;
    private ActionExecutionStatus status;
    private long startedAtMs;
    private long completedAtMs;
    private String failureReason;

    public ActionResponse() {
    }

    public ActionResponse(Uuid actionUuid, AvailabilityActionType actionType, AvailabilityTarget target,
                          ActionExecutionStatus status, long startedAtMs, long completedAtMs, String failureReason) {
        this.actionUuid = actionUuid;
        this.actionType = actionType;
        this.target = target;
        this.status = status;
        this.startedAtMs = startedAtMs;
        this.completedAtMs = completedAtMs;
        this.failureReason = failureReason;
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

    public ActionExecutionStatus getStatus() {
        return status;
    }

    public void setStatus(ActionExecutionStatus status) {
        this.status = status;
    }

    public long getStartedAtMs() {
        return startedAtMs;
    }

    public void setStartedAtMs(long startedAtMs) {
        this.startedAtMs = startedAtMs;
    }

    public long getCompletedAtMs() {
        return completedAtMs;
    }

    public void setCompletedAtMs(long completedAtMs) {
        this.completedAtMs = completedAtMs;
    }

    public String getFailureReason() {
        return failureReason;
    }

    public void setFailureReason(String failureReason) {
        this.failureReason = failureReason;
    }

    public boolean isSuccessfulExecution() {
        return status == ActionExecutionStatus.SUCCEEDED;
    }

    @Override
    public boolean equals(Object o) {
        if (!(o instanceof ActionResponse)) {
            return false;
        }
        ActionResponse that = (ActionResponse) o;
        return schemaVersion == that.schemaVersion &&
            startedAtMs == that.startedAtMs &&
            completedAtMs == that.completedAtMs &&
            Objects.equals(actionUuid, that.actionUuid) &&
            actionType == that.actionType &&
            Objects.equals(target, that.target) &&
            status == that.status &&
            Objects.equals(failureReason, that.failureReason);
    }

    @Override
    public int hashCode() {
        return Objects.hash(schemaVersion, actionUuid, actionType, target, status, startedAtMs, completedAtMs,
            failureReason);
    }
}
