/*
 * Copyright 2024, AutoMQ CO.,LTD.
 *
 * Use of this software is governed by the Business Source License
 * included in the file BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */

package com.automq.stream.s3.wal.benchmark;

import com.automq.stream.s3.ByteBufAlloc;
import com.automq.stream.s3.wal.impl.block.BlockWALService;
import com.automq.stream.s3.wal.WriteAheadLog;
import com.automq.stream.s3.wal.util.WALChannel;
import io.netty.buffer.ByteBuf;
import java.io.File;
import java.io.IOException;
import java.util.Iterator;
import net.sourceforge.argparse4j.helper.HelpScreenException;
import net.sourceforge.argparse4j.inf.ArgumentParser;
import net.sourceforge.argparse4j.inf.ArgumentParserException;
import net.sourceforge.argparse4j.inf.Namespace;

import static com.automq.stream.s3.wal.util.WALUtil.isBlockDevice;

public class BenchTool {

    public static Namespace parseArgs(ArgumentParser parser, String[] args) {
        Namespace ns = null;
        try {
            ns = parser.parseArgs(args);
        } catch (HelpScreenException e) {
            System.exit(0);
        } catch (ArgumentParserException e) {
            parser.handleError(e);
            System.exit(1);
        }
        return ns;
    }

    public static int recoverAndReset(WriteAheadLog wal) {
        int recovered = 0;
        for (Iterator<WriteAheadLog.RecoverResult> it = wal.recover(); it.hasNext(); ) {
            it.next().record().release();
            recovered++;
        }
        wal.reset().join();
        return recovered;
    }

    public static void resetWALHeader(String path) throws IOException {
        System.out.println("Resetting WAL header");
        if (isBlockDevice(path)) {
            // block device
            int capacity = BlockWALService.WAL_HEADER_TOTAL_CAPACITY;
            WALChannel channel = WALChannel.builder(path).capacity(capacity).build();
            channel.open();
            ByteBuf buf = ByteBufAlloc.byteBuffer(capacity);
            buf.writeZero(capacity);
            channel.write(buf, 0);
            buf.release();
            channel.close();
        } else {
            // normal file
            File file = new File(path);
            if (file.isFile() && !file.delete()) {
                throw new IOException("Failed to delete existing file " + file);
            }
        }
    }
}
