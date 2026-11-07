package com.automq.stream.s3.wal.impl.object;

import org.junit.jupiter.api.Test;

import io.netty.buffer.ByteBuf;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class WALObjectHeaderTest {

    @Test
    public void testV0() {
        WALObjectHeader header = new WALObjectHeader(42L, 84L, 126L, 1, 2);
        assertEquals(-1, header.trimOffset());
        assertEquals(WALObjectHeader.WAL_HEADER_MAGIC_CODE_V0, header.magicCode());

        ByteBuf buffer = header.marshal();
        assertEquals(WALObjectHeader.WAL_HEADER_SIZE_V0, buffer.readableBytes());

        WALObjectHeader unmarshal = WALObjectHeader.unmarshal(buffer);
        assertEquals(header, unmarshal);
    }

    @Test
    public void testV1() {
        WALObjectHeader header = new WALObjectHeader(42L, 84L, 126L, 1, 2, 168L);
        assertEquals(WALObjectHeader.WAL_HEADER_MAGIC_CODE_V1, header.magicCode());

        ByteBuf buffer = header.marshal();
        assertEquals(WALObjectHeader.WAL_HEADER_SIZE_V1, buffer.readableBytes());

        WALObjectHeader unmarshal = WALObjectHeader.unmarshal(buffer);
        assertEquals(header, unmarshal);
    }
}
