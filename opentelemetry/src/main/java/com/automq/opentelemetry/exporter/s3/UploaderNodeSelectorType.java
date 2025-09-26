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

package com.automq.opentelemetry.exporter.s3;

import java.util.HashMap;
import java.util.Map;

/**
 * Enum representing the type of uploader node selector.
 * Provides type safety and common operations for selector types.
 */
public enum UploaderNodeSelectorType {
    /**
     * Static selector - uses a fixed configuration value.
     */
    STATIC("static"),
    
    /**
     * Node ID based selector - selects based on node ID matching.
     */
    NODE_ID("nodeid"),
    
    /**
     * File-based leader election selector - uses a file for leader election.
     */
    FILE("file"),
    
    /**
     * Custom selector type - used for SPI-provided selectors.
     */
    CUSTOM(null);
    
    private final String type;
    private static final Map<String, UploaderNodeSelectorType> TYPE_MAP = new HashMap<>();
    
    static {
        for (UploaderNodeSelectorType value : values()) {
            if (value != CUSTOM) {
                TYPE_MAP.put(value.type, value);
            }
        }
    }
    
    UploaderNodeSelectorType(String type) {
        this.type = type;
    }
    
    /**
     * Gets the string representation of this selector type.
     * 
     * @return The type string
     */
    public String getType() {
        return type;
    }
    
    /**
     * Converts a string to the appropriate selector type enum.
     * 
     * @param typeString The type string to convert
     * @return The matching selector type or CUSTOM if no built-in match
     */
    public static UploaderNodeSelectorType fromString(String typeString) {
        if (typeString == null) {
            return STATIC; // Default
        }
        
        return TYPE_MAP.getOrDefault(typeString.toLowerCase(), CUSTOM);
    }
    
    /**
     * Creates a CUSTOM type with a specific value.
     * 
     * @param customType The custom type string
     * @return A CUSTOM type instance
     */
    public static UploaderNodeSelectorType customType(String customType) {
        return CUSTOM;
    }
}
