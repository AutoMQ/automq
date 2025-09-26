package com.automq.log.uploader.selector;

import java.util.HashMap;
import java.util.Locale;
import java.util.Map;

/**
 * Supported selector types.
 */
public enum LogUploaderNodeSelectorType {
    STATIC("static"),
    NODE_ID("nodeid"),
    FILE("file"),
    CUSTOM(null);

    private static final Map<String, LogUploaderNodeSelectorType> LOOKUP = new HashMap<>();

    static {
        for (LogUploaderNodeSelectorType value : values()) {
            if (value.type != null) {
                LOOKUP.put(value.type, value);
            }
        }
    }

    private final String type;

    LogUploaderNodeSelectorType(String type) {
        this.type = type;
    }

    public String getType() {
        return type;
    }

    public static LogUploaderNodeSelectorType fromString(String type) {
        if (type == null) {
            return STATIC;
        }
        return LOOKUP.getOrDefault(type.toLowerCase(Locale.ROOT), CUSTOM);
    }
}
