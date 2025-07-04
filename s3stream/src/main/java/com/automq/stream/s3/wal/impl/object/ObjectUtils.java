package com.automq.stream.s3.wal.impl.object;

import com.automq.stream.s3.operator.ObjectStorage;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ObjectUtils {
    private static final Logger LOGGER = LoggerFactory.getLogger(ObjectUtils.class);
    public static final String OBJECT_PATH_OFFSET_DELIMITER = "-";

    public static List<WALObject> parse(List<ObjectStorage.ObjectInfo> objects) {
        List<WALObject> walObjects = new ArrayList<>(objects.size());
        for (ObjectStorage.ObjectInfo object : objects) {
            // v0: md5hex(nodeId)/clusterId/nodeId/epoch/wal/startOffset
            // v1: md5hex(nodeId)/clusterId/nodeId/epoch/wal/startOffset_endOffset
            String path = object.key();
            String[] parts = path.split("/");
            try {
                long epoch = Long.parseLong(parts[parts.length - 3]);
                long length = object.size();
                String rawOffset = parts[parts.length - 1];

                WALObject walObject;
                if (rawOffset.contains(OBJECT_PATH_OFFSET_DELIMITER)) {
                    // v1 format: {startOffset}-{endOffset}
                    long startOffset = Long.parseLong(rawOffset.substring(0, rawOffset.indexOf(OBJECT_PATH_OFFSET_DELIMITER)));
                    long endOffset = Long.parseLong(rawOffset.substring(rawOffset.indexOf(OBJECT_PATH_OFFSET_DELIMITER) + 1));
                    walObject = new WALObject(object.bucketId(), path, epoch, startOffset, endOffset, length);
                } else {
                    // v0 format: {startOffset}
                    long startOffset = Long.parseLong(rawOffset);
                    walObject = new WALObject(object.bucketId(), path, epoch, startOffset, length);
                }
                walObjects.add(walObject);
            } catch (NumberFormatException e) {
                // Ignore invalid path
                LOGGER.warn("Invalid WAL object: {}", path);
            }
        }
        walObjects.sort(Comparator.comparingLong(WALObject::epoch).thenComparingLong(WALObject::startOffset));
        return walObjects;
    }

    /**
     * Remove overlap objects.
     * @return overlap objects.
     */
    public static List<WALObject> skipOverlapObjects(List<WALObject> objects) {
        List<WALObject> overlapObjects = new ArrayList<>();
        WALObject lastObject = null;
        for (WALObject object : objects) {
            if (lastObject == null) {
                lastObject = object;
                continue;
            }
            if (lastObject.epoch() != object.epoch()) {
                if (lastObject.endOffset() > object.startOffset()) {
                    // maybe the old epoch node write dirty object after it was fenced.
                    overlapObjects.add(lastObject);
                }
            }
        }
        overlapObjects.forEach(objects::remove);
        return overlapObjects;
    }
}
