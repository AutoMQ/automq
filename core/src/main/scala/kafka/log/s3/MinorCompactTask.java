package kafka.log.s3;

import kafka.log.s3.model.StreamRecordBatch;
import kafka.log.s3.objects.CommitCompactObjectRequest;
import kafka.log.s3.objects.ObjectManager;
import kafka.log.s3.objects.ObjectStreamRange;
import kafka.log.s3.objects.StreamObject;
import kafka.log.s3.operator.S3Operator;
import kafka.log.s3.utils.ObjectUtils;
import org.apache.kafka.common.utils.ThreadUtils;

import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;
import java.util.stream.Stream;

class MinorCompactTask implements Runnable {
    private static final long NOOP_TIMESTAMP = -1L;
    private final long compactSizeThreshold;
    private final long maxCompactInterval;
    private final int streamSplitSizeThreshold;
    private final BlockingQueue<MinorCompactPart> waitingCompactRecords = new LinkedBlockingQueue<>();
    private final AtomicLong waitingCompactRecordsBytesSize = new AtomicLong();
    private volatile long lastCompactTimestamp = NOOP_TIMESTAMP;
    private final ObjectManager objectManager;
    private final S3Operator s3Operator;
    private final ScheduledExecutorService schedule = Executors.newSingleThreadScheduledExecutor(
            ThreadUtils.createThreadFactory("minor compact", true));

    public MinorCompactTask(long compactSizeThreshold, long maxCompactInterval, int streamSplitSizeThreshold, ObjectManager objectManager, S3Operator s3Operator) {
        this.compactSizeThreshold = compactSizeThreshold;
        this.maxCompactInterval = maxCompactInterval;
        this.streamSplitSizeThreshold = streamSplitSizeThreshold;
        // TODO: close
        schedule.scheduleAtFixedRate(this, 1, 1, TimeUnit.SECONDS);
        this.objectManager = objectManager;
        this.s3Operator = s3Operator;
    }

    public void tryCompact(MinorCompactPart part) {
        // TODO: back pressure
        if (lastCompactTimestamp == NOOP_TIMESTAMP) {
            lastCompactTimestamp = System.currentTimeMillis();
        }
        waitingCompactRecords.add(part);
        if (waitingCompactRecordsBytesSize.addAndGet(part.size) >= compactSizeThreshold) {
            schedule.execute(this::tryCompact0);
        }
    }

    @Override
    public void run() {
        tryCompact0();
    }

    private void tryCompact0() {
        long now = System.currentTimeMillis();
        boolean timeout = lastCompactTimestamp != NOOP_TIMESTAMP && (now - lastCompactTimestamp) >= maxCompactInterval;
        boolean sizeExceed = waitingCompactRecordsBytesSize.get() >= compactSizeThreshold;
        if (!sizeExceed && !timeout) {
            return;
        }
        try {
            CommitCompactObjectRequest compactReq = new CommitCompactObjectRequest();
            List<MinorCompactPart> parts = new ArrayList<>(waitingCompactRecords.size());

            waitingCompactRecords.drainTo(parts);

            lastCompactTimestamp = now;
            waitingCompactRecordsBytesSize.getAndAdd(-parts.stream().mapToLong(r -> r.size).sum());

            compactReq.setCompactedObjectIds(parts.stream().map(p -> p.walObjectId).collect(Collectors.toList()));

            long objectId = objectManager.prepareObject(1, TimeUnit.SECONDS.toMillis(30)).get();
            compactReq.setObjectId(objectId);
            String objectKey = ObjectUtils.genKey(0, "todocluster", objectId);
            ObjectWriter minorCompactObject = new ObjectWriter(objectKey, s3Operator);
            List<List<StreamRecordBatch>> streamRecordsList = sortAndSplit(parts);
            for (List<StreamRecordBatch> streamRecords : streamRecordsList) {
                long streamSize = streamRecords.stream().mapToLong(r -> r.getRecordBatch().rawPayload().remaining()).sum();
                if (streamSize >= streamSplitSizeThreshold) {
                    long streamObjectId = objectManager.prepareObject(1, TimeUnit.SECONDS.toMillis(30)).get();
                    String streamObjectKey = ObjectUtils.genKey(0, "todocluster", streamObjectId);
                    ObjectWriter streamObjectWriter = new ObjectWriter(streamObjectKey, s3Operator);
                    for (StreamRecordBatch record: streamRecords) {
                        streamObjectWriter.write(record);
                    }
                    // TODO: parallel
                    streamObjectWriter.close().get();
                    long streamId = streamRecords.get(0).getStreamId();
                    long startOffset = streamRecords.get(0).getBaseOffset();
                    long endOffset = streamRecords.get(streamRecords.size() -1).getLastOffset();
                    StreamObject streamObject = new StreamObject();
                    streamObject.setObjectId(streamObjectId);
                    streamObject.setObjectSize(streamObjectWriter.size());
                    streamObject.setStreamId(streamId);
                    streamObject.setStartOffset(startOffset);
                    streamObject.setEndOffset(endOffset);
                    compactReq.addStreamObject(streamObject);
                } else {
                    for (StreamRecordBatch record: streamRecords) {
                        minorCompactObject.write(record);
                    }
                    long streamId = streamRecords.get(0).getStreamId();
                    long startOffset = streamRecords.get(0).getBaseOffset();
                    long endOffset = streamRecords.get(streamRecords.size() -1).getLastOffset();
                    compactReq.addStreamRange(new ObjectStreamRange(streamId, -1L, startOffset, endOffset));
                    // minor compact object block only contain single stream's data.
                    minorCompactObject.closeCurrentBlock();
                }
            }
            minorCompactObject.close().get();
            compactReq.setObjectSize(minorCompactObject.size());
        } catch (Throwable e) {
            //TODO: retry
        }

    }

    private List<List<StreamRecordBatch>> sortAndSplit(List<MinorCompactPart> parts) {
        int count = parts.stream().mapToInt(p -> p.records.size()).sum();
        // TODO: more efficient sort
        List<StreamRecordBatch> sortedList = new ArrayList<>(count);
        for (MinorCompactPart part: parts) {
            sortedList.addAll(part.records);
        }
        Collections.sort(sortedList);
        List<List<StreamRecordBatch>> streamRecordsList = new ArrayList<>(1024);
        long streamId = -1L;
        List<StreamRecordBatch> streamRecords = null;
        // TODO: seperate different epoch
        for (StreamRecordBatch record: sortedList) {
            long recordStreamId = record.getStreamId();
            if (recordStreamId != streamId) {
                if (streamRecords != null) {
                    streamRecordsList.add(streamRecords);
                }
                streamRecords = new LinkedList<>();
                streamId = recordStreamId;
            }
            if (streamRecords != null) {
                streamRecords.add(record);
            }
        }
        if (streamRecords != null) {
            streamRecordsList.add(streamRecords);
        }
        return streamRecordsList;
    }

    static class MinorCompactPart {
        long walObjectId;
        List<StreamRecordBatch> records;
        long size;

        public MinorCompactPart(long walObjectId, List<StreamRecordBatch> records) {
            this.walObjectId = walObjectId;
            this.records = new ArrayList<>(records);
            this.size = records.stream().mapToLong(r -> r.getRecordBatch().rawPayload().remaining()).sum();
        }
    }
}
