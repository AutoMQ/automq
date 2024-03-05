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

package kafka.autobalancer.model;

import kafka.autobalancer.common.Resource;

public class ModelUtils {

    public static void moveReplicaLoad(BrokerUpdater.Broker src, BrokerUpdater.Broker dest,
                                       TopicPartitionReplicaUpdater.TopicPartitionReplica replica) {
        for (Resource resource : replica.getResources()) {
            double delta = replica.load(resource);
            src.setLoad(resource, src.load(resource) - delta);
            dest.setLoad(resource, dest.load(resource) + delta);
        }
    }

}
