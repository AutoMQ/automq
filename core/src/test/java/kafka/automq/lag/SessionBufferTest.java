package kafka.automq.lag;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class SessionBufferTest {

    private SessionBuffer buffer;

    @BeforeEach
    void setUp() {
        buffer = new SessionBuffer(10, 1000L);
    }

    @Test
    void testAppendAndLookupExact() {
        buffer.append(100, 10000L, 10000L);
        SessionBuffer.LookupResult result = buffer.lookup(100);
        assertEquals(10000L, result.timestamp());
        assertTrue(result.precise());
    }

    @Test
    void testAppendFiltersTimeGap() {
        buffer.append(100, 10000L, 1000L);
        buffer.append(200, 20000L, 1500L);
        buffer.append(300, 30000L, 2500L);

        assertEquals(2, buffer.size());
    }

    @Test
    void testLookupInterpolation() {
        buffer.append(100, 10000L, 0L);
        buffer.append(300, 30000L, 2000L);

        SessionBuffer.LookupResult result = buffer.lookup(200);
        assertEquals(20000L, result.timestamp());
        assertFalse(result.precise());
    }

    @Test
    void testLookupMiss() {
        SessionBuffer.LookupResult result = buffer.lookup(100);
        assertEquals(-1, result.timestamp());
    }

    @Test
    void testCompactionOnOverflow() {
        for (int i = 0; i <= 10; i++) {
            buffer.append(i * 100L, i * 10000L, i * 2000L);
        }
        assertEquals(5, buffer.size(), "Buffer should compact to maxSize/2");
        SessionBuffer.LookupResult first = buffer.lookup(0);
        assertNotEquals(-1, first.timestamp());
        SessionBuffer.LookupResult last = buffer.lookup(1000);
        assertNotEquals(-1, last.timestamp());
    }

    @Test
    void testGetRepresentativePoints() {
        buffer.append(100, 10000L, 0L);
        buffer.append(200, 20000L, 2000L);
        buffer.append(300, 30000L, 4000L);
        buffer.append(400, 40000L, 6000L);

        List<SessionBuffer.DataPoint> points = buffer.getRepresentativePoints(2);
        assertEquals(2, points.size());
        assertEquals(100, points.get(0).offset());
        assertEquals(400, points.get(points.size() - 1).offset());
    }

    @Test
    void testMinMaxOffset() {
        buffer.append(200, 20000L, 0L);
        buffer.append(300, 30000L, 2000L);
        buffer.append(500, 50000L, 4000L);

        assertEquals(200, buffer.minOffset());
        assertEquals(500, buffer.maxOffset());
    }

    @Test
    void testAppendNonMonotonicOffsetResetsBuffer() {
        buffer.append(100, 10000L, 0L);
        buffer.append(500, 50000L, 2000L);
        buffer.append(200, 20000L, 4000L);

        assertEquals(1, buffer.size());
        assertEquals(200L, buffer.minOffset());
        assertEquals(200L, buffer.maxOffset());

        SessionBuffer.LookupResult result = buffer.lookup(200L);
        assertEquals(20000L, result.timestamp());
        assertTrue(result.precise());
    }

    @Test
    void testLookupAfterSeekReset() {
        buffer.append(100, 10000L, 0L);
        buffer.append(300, 30000L, 2000L);
        buffer.append(200, 20000L, 4000L);
        buffer.append(400, 40000L, 6000L);

        SessionBuffer.LookupResult result = buffer.lookup(300L);
        assertEquals(30000L, result.timestamp());
        assertFalse(result.precise());
    }
}
