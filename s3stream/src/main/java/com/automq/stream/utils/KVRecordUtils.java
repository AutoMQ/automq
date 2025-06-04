package com.automq.stream.utils;

public class KVRecordUtils {

    public static String buildCompositeKey(String namespace, String key) {
        if (namespace == null || namespace.isEmpty()) {
            return key;
        }
        return namespace + "/" + key;
    }
}
