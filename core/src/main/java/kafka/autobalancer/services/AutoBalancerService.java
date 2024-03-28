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

package kafka.autobalancer.services;

import com.automq.stream.utils.LogContext;
import org.apache.kafka.common.Reconfigurable;

public abstract class AutoBalancerService extends AbstractResumableService implements Reconfigurable {
    public AutoBalancerService(LogContext logContext) {
        super(logContext);
    }
}
