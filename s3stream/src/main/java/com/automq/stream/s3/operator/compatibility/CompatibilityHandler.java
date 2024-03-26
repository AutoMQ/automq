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

package com.automq.stream.s3.operator.compatibility;


import com.automq.stream.s3.Config;
import com.google.common.base.Strings;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CompatibilityHandler {
    public static final CompatibilityHandler NOOP = new CompatibilityHandler();

    private static final Logger LOGGER = LoggerFactory.getLogger(CompatibilityHandler.class);
    public final DeleteObjectsCompatibilityHandler deleteObjectsCompatibilityHandler;

    public CompatibilityHandler() {
        this(null);
    }

    public CompatibilityHandler(Config c) {
        if (c == null) {
            deleteObjectsCompatibilityHandler = null;
            return;
        }

        if (Strings.isNullOrEmpty(c.deleteObjectsCompatibilityHandlerClassName())) {
            deleteObjectsCompatibilityHandler = null;
        } else {
            try {
                deleteObjectsCompatibilityHandler = (DeleteObjectsCompatibilityHandler) Class.forName(c.deleteObjectsCompatibilityHandlerClassName()).getDeclaredConstructor().newInstance();
                LOGGER.info("[CompatibilityHandler] create deleteObjectsCompatibilityHandler {}", c.deleteObjectsCompatibilityHandlerClassName());
            } catch (Exception e) {
                LOGGER.error("[CompatibilityHandler] error when create new deleteObjectsCompatibilityHandler name {}", c.deleteObjectsCompatibilityHandlerClassName(), e);
                throw new RuntimeException(e);
            }
        }
    }
}

