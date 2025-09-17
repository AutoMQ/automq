package com.automq.stream.s3.metrics.wrapper;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Random;

public class DeltaHistogramTest {

    @Test
    public void test() throws InterruptedException {
        Random random = new Random();
        DeltaHistogram deltaHistogram = new DeltaHistogram(1000L);
        Thread thread = new Thread(() -> {
            while (true) {
                deltaHistogram.record(10 + random.nextInt(10000));
                try {
                    Thread.sleep(1);
                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                }
            }
        });
        thread.setDaemon(true);
        thread.start();

        for (int i = 0; i < 3; i++) {
            logIt(deltaHistogram);
            Thread.sleep(1000);
        }
        DeltaHistogram.SnapshotExt snapshotExt = deltaHistogram.snapshotAndReset();
        Assertions.assertTrue(snapshotExt.getCount() > 0);
        Assertions.assertTrue(snapshotExt.mean() > 0);
        Assertions.assertTrue(snapshotExt.getMax() > 0);
        Assertions.assertTrue(snapshotExt.getP99() > 0);
    }

    public static void logIt(DeltaHistogram deltaHistogram) {
        long recordSizeBytes = 2048;
        DeltaHistogram.SnapshotExt snapshotExt = deltaHistogram.snapshotAndReset();
        System.out.printf("Append task | Append Rate %d msg/s %d KB/s | Avg Latency %.3f ms | Max Latency %d ms | P99 Latency %.3f ms\n",
            snapshotExt.getCount() * 1000 / deltaHistogram.getSnapshotInterval(),
            snapshotExt.getCount() * 1000 / deltaHistogram.getSnapshotInterval() * recordSizeBytes / 1024,
            snapshotExt.mean(),
            snapshotExt.getMax(),
            snapshotExt.getP99());
    }
}
