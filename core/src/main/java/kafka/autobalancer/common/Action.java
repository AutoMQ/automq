/*
 * Copyright 2024, AutoMQ CO.,LTD.
 *
 * Use of this software is governed by the Business Source License
 * included in the file BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */

package kafka.autobalancer.common;

import java.util.Objects;
import org.apache.kafka.common.TopicPartition;

public class Action {
    private final TopicPartition srcTp;
    private final TopicPartition destTp;
    private final int srcBrokerId;
    private int destBrokerId;

    private final ActionType type;

    public Action(ActionType type, TopicPartition srcTp, int srcBrokerId, int destBrokerId) {
        this(type, srcTp, srcBrokerId, destBrokerId, null);
    }

    public Action(ActionType type, TopicPartition srcTp, int srcBrokerId, int destBrokerId, TopicPartition destTp) {
        this.type = type;
        this.srcTp = srcTp;
        this.srcBrokerId = srcBrokerId;
        this.destBrokerId = destBrokerId;
        this.destTp = destTp;
    }

    public ActionType getType() {
        return type;
    }

    public TopicPartition getSrcTopicPartition() {
        return srcTp;
    }

    public TopicPartition getDestTopicPartition() {
        return destTp;
    }

    public void setDestBrokerId(int destBrokerId) {
        this.destBrokerId = destBrokerId;
    }

    public int getDestBrokerId() {
        return destBrokerId;
    }

    public int getSrcBrokerId() {
        return srcBrokerId;
    }

    public Action undo() {
        if (type == ActionType.MOVE) {
            return new Action(type, srcTp, destBrokerId, srcBrokerId);
        }
        return new Action(type, destTp, destBrokerId, srcBrokerId, srcTp);
    }

    @Override
    public String toString() {
        return "Action{" +
                "srcTp=" + srcTp +
                ", destTp=" + destTp +
                ", srcBrokerId=" + srcBrokerId +
                ", destBrokerId=" + destBrokerId +
                ", type=" + type +
                '}';
    }

    public String prettyString() {
        if (this.type == ActionType.MOVE) {
            return String.format("Action-%s: %s@node-%d ---> node-%d", type, srcTp, srcBrokerId, destBrokerId);
        } else if (this.type == ActionType.SWAP) {
            return String.format("Action-%s: %s@node-%d <--> %s@node-%d", type, srcTp, srcBrokerId, destTp, destBrokerId);
        }
        return toString();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        Action action = (Action) o;
        return srcBrokerId == action.srcBrokerId && destBrokerId == action.destBrokerId
                && Objects.equals(srcTp, action.srcTp) && Objects.equals(destTp, action.destTp) && type == action.type;
    }

    @Override
    public int hashCode() {
        return Objects.hash(srcTp, destTp, srcBrokerId, destBrokerId, type);
    }
}
