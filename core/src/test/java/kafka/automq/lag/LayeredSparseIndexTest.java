package kafka.automq.lag;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class LayeredSparseIndexTest {

    // 2-level index for most tests: L0(100ms interval, cap=4), L1(500ms interval, cap=4)
    private LayeredSparseIndex index;

    @BeforeEach
    void setUp() {
        index = new LayeredSparseIndex(
            new long[]{100L, 500L},
            new int[]{4, 4}
        );
    }

    @Test
    void testInsertSinglePoint() {
        index.insert(100, 1000L);
        assertEquals(1, index.totalSize());
        assertEquals(1, index.levelSize(0));
    }

    @Test
    void testInsertIgnoresDuplicateOrLowerOffset() {
        index.insert(100, 1000L);
        index.insert(100, 2000L);  // same offset, ignored
        index.insert(50, 500L);    // lower offset, ignored
        assertEquals(1, index.totalSize());
    }

    @Test
    void testInsertRespectsTimestampInterval() {
        index.insert(100, 1000L);
        index.insert(200, 1050L);  // only 50ms gap, < 100ms interval -> skipped
        assertEquals(1, index.totalSize());

        index.insert(300, 1100L);  // 100ms gap -> accepted
        assertEquals(2, index.totalSize());
    }

    @Test
    void testInsertNonMonotonicTimestamp() {
        index.insert(100, 1000L);
        index.insert(200, 900L);   // timestamp goes backward, but abs(900-1000)=100 >= interval
        assertEquals(2, index.totalSize());
    }

    @Test
    void testOverflowDemotesToNextLevel() {
        // L0 cap=4, insert 5 points with sufficient timestamp gaps
        index.insert(100, 1000L);
        index.insert(200, 1200L);
        index.insert(300, 1400L);
        index.insert(400, 1600L);
        assertEquals(4, index.levelSize(0));

        index.insert(500, 1800L);  // triggers overflow: remove first 2 from L0, demote to L1
        assertTrue(index.levelSize(0) <= 4, "L0 should be within capacity after overflow");
        assertTrue(index.levelSize(1) > 0, "L1 should have received demoted points");
        assertTrue(index.totalSize() <= 5, "Total points should stay bounded after overflow");
    }

    @Test
    void testOverflowRecursesMultipleLevels() {
        // 3-level index: L0(10ms,3), L1(50ms,3), L2(200ms,3)
        LayeredSparseIndex idx = new LayeredSparseIndex(
            new long[]{10L, 50L, 200L},
            new int[]{3, 3, 3}
        );

        // Fill enough to trigger cascading overflow
        long ts = 0;
        for (int i = 0; i < 20; i++) {
            idx.insert(i * 10L, ts);
            ts += 100L;
        }

        assertTrue(idx.levelSize(2) > 0, "L2 should have points from cascaded overflow");
        assertTrue(idx.totalSize() <= 9, "Total should not exceed sum of capacities");
    }

    @Test
    void testLookupExactHit() {
        index.insert(100, 1000L);
        index.insert(200, 1200L);

        LayeredSparseIndex.LookupBounds bounds = index.lookupBounds(100);
        assertNotNull(bounds.floor());
        assertEquals(100, bounds.floor().offset());
        assertEquals(1000L, bounds.floor().timestamp());
        // exact hit: floor == ceil
        assertEquals(bounds.floor(), bounds.ceil());
    }

    @Test
    void testLookupInterpolation() {
        index.insert(100, 1000L);
        index.insert(200, 2000L);

        LayeredSparseIndex.LookupBounds bounds = index.lookupBounds(150);
        assertNotNull(bounds.floor());
        assertNotNull(bounds.ceil());
        assertEquals(100, bounds.floor().offset());
        assertEquals(200, bounds.ceil().offset());
    }

    @Test
    void testLookupMissOnEmpty() {
        LayeredSparseIndex.LookupBounds bounds = index.lookupBounds(100);
        assertNull(bounds.floor());
        assertNull(bounds.ceil());
    }

    @Test
    void testLookupFloorOnlyBeyondMax() {
        index.insert(100, 1000L);
        index.insert(200, 1200L);

        LayeredSparseIndex.LookupBounds bounds = index.lookupBounds(300);
        assertNotNull(bounds.floor());
        assertEquals(200, bounds.floor().offset());
        assertNull(bounds.ceil());
    }

    @Test
    void testLookupCrossLevel() {
        // Force points into different levels via overflow, then query between them
        index.insert(100, 1000L);
        index.insert(200, 1200L);
        index.insert(300, 1400L);
        index.insert(400, 1600L);
        index.insert(500, 1800L);  // overflow moves early points to L1

        // Query an offset that might have floor in L1 and ceil in L0
        LayeredSparseIndex.LookupBounds bounds = index.lookupBounds(250);
        assertNotNull(bounds.floor());
        assertNotNull(bounds.ceil());
        assertTrue(bounds.floor().offset() <= 250);
        assertTrue(bounds.ceil().offset() >= 250);
    }

    @Test
    void testReset() {
        index.insert(100, 1000L);
        index.insert(200, 1200L);
        assertFalse(index.isEmpty());

        index.reset();
        assertTrue(index.isEmpty());
        assertEquals(0, index.totalSize());

        // Can insert again after reset (lastInsertedOffset is reset)
        index.insert(50, 500L);
        assertEquals(1, index.totalSize());
    }

    @Test
    void testMinMaxOffset() {
        index.insert(100, 1000L);
        index.insert(200, 1200L);
        index.insert(300, 1400L);

        assertEquals(100, index.minOffset());
        assertEquals(300, index.maxOffset());
    }

    @Test
    void testMinMaxOffsetAfterOverflow() {
        index.insert(100, 1000L);
        index.insert(200, 1200L);
        index.insert(300, 1400L);
        index.insert(400, 1600L);
        index.insert(500, 1800L);  // overflow

        assertEquals(100, index.minOffset(), "Min should track across levels");
        assertEquals(500, index.maxOffset());
    }

    @Test
    void testEmptyMinMaxOffset() {
        assertEquals(Long.MAX_VALUE, index.minOffset());
        assertEquals(Long.MIN_VALUE, index.maxOffset());
    }

    @Test
    void testOverflowCompactsArray() {
        // After overflow, remaining L0 points should be in contiguous array from index 0
        index.insert(100, 1000L);
        index.insert(200, 1200L);
        index.insert(300, 1400L);
        index.insert(400, 1600L);
        index.insert(500, 1800L);

        // Lookup should work correctly (binary search on contiguous array)
        LayeredSparseIndex.LookupBounds bounds = index.lookupBounds(350);
        assertNotNull(bounds.floor());
        assertNotNull(bounds.ceil());
    }

    @Test
    void testDemoteRespectsNextLevelInterval() {
        // L0(100ms, 4), L1(500ms, 4)
        // Insert points with 100ms gaps into L0, overflow to L1
        // L1 has 500ms interval, so not all demoted points will be kept
        index.insert(100, 1000L);
        index.insert(200, 1100L);
        index.insert(300, 1200L);
        index.insert(400, 1300L);
        index.insert(500, 1400L);  // overflow

        // L1 should only keep points that satisfy 500ms interval
        assertTrue(index.levelSize(1) <= 2,
            "L1 should filter points by its own interval constraint");
    }

    @Test
    void testSingleLevelIndex() {
        LayeredSparseIndex single = new LayeredSparseIndex(
            new long[]{100L},
            new int[]{4}
        );

        single.insert(100, 1000L);
        single.insert(200, 1200L);
        single.insert(300, 1400L);
        single.insert(400, 1600L);
        single.insert(500, 1800L);  // overflow: first 2 discarded (no next level)

        assertTrue(single.totalSize() <= 4);

        LayeredSparseIndex.LookupBounds bounds = single.lookupBounds(400);
        assertNotNull(bounds.floor());
    }

    @Test
    void testDemoteAppendNeverExceedsLevelCapacityBuffer() {
        LayeredSparseIndex idx = new LayeredSparseIndex(
            new long[]{0L, 0L, 0L},
            new int[]{2, 2, 2}
        );

        assertDoesNotThrow(() -> {
            for (int i = 0; i < 200; i++) {
                idx.insert(i, i);
            }
        });
        assertTrue(idx.totalSize() <= 6);
    }
}
