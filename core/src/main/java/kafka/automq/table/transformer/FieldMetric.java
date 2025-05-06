package kafka.automq.table.transformer;

import java.nio.ByteBuffer;

public class FieldMetric {

    public static int count(String value) {
        if (value == null) {
            return 0;
        }
        return Math.max((value.length() + 23) / 24, 1);
    }

    public static int count(ByteBuffer value) {
        if (value == null) {
            return 0;
        }
        return Math.max((value.remaining() + 31) / 32, 1);
    }

    public static int count(byte[] value) {
        if (value == null) {
            return 0;
        }
        return Math.max((value.length + 31) / 32, 1);
    }

}
