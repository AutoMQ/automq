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
package com.automq.stream.thirdparty.moe.cnkirito.kdio;

import com.sun.jna.Native;
import com.sun.jna.NativeLong;
import com.sun.jna.Platform;
import com.sun.jna.Pointer;
import com.sun.jna.ptr.PointerByReference;
import io.netty.util.internal.PlatformDependent;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Class containing native hooks and utility methods for performing direct I/O, using
 * the Linux <tt>O_DIRECT</tt> flag.
 * <p>
 * This class is initialized at class load time, by registering JNA hooks into native methods.
 * It also calculates Linux kernel version-dependent alignment amount (in bytes) for use with the <tt>O_DIRECT</tt> flag,
 * when given a string for a file or directory.
 */
public class DirectIOLib {
    static final int PC_REC_XFER_ALIGN = 0x11;
    private static final Logger LOGGER = LoggerFactory.getLogger(DirectIOLib.class);
    public static boolean binit;

    static {
        binit = false;
        try {
            if (!Platform.isLinux()) {
                LOGGER.warn("Not running Linux, jaydio support disabled");
            } else { // now check to see if we have O_DIRECT...

                final int linuxVersion = 0;
                final int majorRev = 1;
                final int minorRev = 2;

                List<Integer> versionNumbers = new ArrayList<Integer>();
                for (String v : System.getProperty("os.version").split("[.\\-]")) {
                    if (v.matches("\\d")) {
                        versionNumbers.add(Integer.parseInt(v));
                    }
                }

                /* From "man 2 open":
                 *
                 * O_DIRECT support was added under Linux in kernel version 2.4.10.  Older Linux kernels simply ignore this flag.  Some file systems may not implement
                 * the flag and open() will fail with EINVAL if it is used.
                 */

                // test to see whether kernel version >= 2.4.10
                if (versionNumbers.get(linuxVersion) > 2) {
                    binit = true;
                } else if (versionNumbers.get(linuxVersion) == 2) {
                    if (versionNumbers.get(majorRev) > 4) {
                        binit = true;
                    } else if (versionNumbers.get(majorRev) == 4 && versionNumbers.get(minorRev) >= 10) {
                        binit = true;
                    }
                }

                if (binit) {
                    // get access to open(), pread(), etc
                    Native.register(Platform.C_LIBRARY_NAME);
                } else {
                    LOGGER.warn(String.format("O_DIRECT not supported on your version of Linux: %d.%d.%d", linuxVersion, majorRev, minorRev));
                }
            }
        } catch (Throwable e) {
            LOGGER.warn("Unable to register libc at class load time: " + e.getMessage(), e);
        }
    }

    private final int fsBlockSize;
    private final long fsBlockNotMask;

    public DirectIOLib(int fsBlockSize) {
        this.fsBlockSize = fsBlockSize;
        this.fsBlockNotMask = -((long) fsBlockSize);
    }

    /**
     * Static method to register JNA hooks for doing direct I/O
     *
     * @param workingDir A directory within the mounted file system on which we'll be working
     *                   Should preferably BE the directory in which we'll be working.
     */
    public static DirectIOLib getLibForPath(String workingDir) {
        int fsBlockSize = initializeSoftBlockSize(workingDir);
        if (fsBlockSize == -1) {
            LOGGER.warn("O_DIRECT support non available on your version of Linux (" + System.getProperty("os.version") + "), " +
                "please upgrade your kernel in order to use jaydio.");
            return null;
        }
        return new DirectIOLib(fsBlockSize);
    }

    /**
     * Finds a block size for use with O_DIRECT. Choose it in the most paranoid
     * way possible to maximize probability that things work.
     *
     * @param fileOrDir A file or directory within which O_DIRECT access will be performed.
     */
    private static int initializeSoftBlockSize(String fileOrDir) {

        int fsBlockSize = -1;

        if (binit) {
            // get file system block size for use with workingDir
            // see "man 3 posix_memalign" for why we do this
            fsBlockSize = pathconf(fileOrDir, PC_REC_XFER_ALIGN);
            /* conservative for version >= 2.6
             * "man 2 open":
             *
             * Under Linux 2.6, alignment
             * to 512-byte boundaries suffices.
             */

            // Since O_DIRECT requires pages to be memory aligned with the file system block size,
            // we will do this too in case the page size and the block size are different for
            // whatever reason. By taking the least common multiple, everything should be happy:
            int pageSize = getpagesize();
            fsBlockSize = lcm(fsBlockSize, pageSize);

            // just being completely paranoid:
            // (512 is the rule for 2.6+ kernels as mentioned before)
            fsBlockSize = lcm(fsBlockSize, 512);

            // lastly, a sanity check
            if (fsBlockSize <= 0 || ((fsBlockSize & (fsBlockSize - 1)) != 0)) {
                LOGGER.warn("file system block size should be a power of two, was found to be " + fsBlockSize);
                LOGGER.warn("Disabling O_DIRECT support");
                return -1;
            }
        }

        return fsBlockSize;
    }

    // -- Java interfaces to native methods

    /**
     * Hooks into errno using Native.getLastError(), and parses it with native strerror function.
     *
     * @return An error message corresponding to the last <tt>errno</tt>
     */
    public static String getLastError() {
        return strerror(Native.getLastError());
    }

    /**
     * Static variant of {@link #blockEnd(int)}.
     *
     * @param blockSize
     * @param position
     * @return The smallest number greater than or equal to <tt>position</tt>
     * which is a multiple of the <tt>blockSize</tt>
     */
    public static long blockEnd(int blockSize, long position) {
        long ceil = (position + blockSize - 1) / blockSize;
        return ceil * blockSize;
    }

    /**
     * Euclid's algo for gcd is more general than we need
     * since we only have powers of 2, but w/e
     *
     * @param x
     * @param y
     * @return The least common multiple of <tt>x</tt> and <tt>y</tt>
     */
    public static int lcm(long x, long y) {
        // will hold gcd
        long g = x;
        long yc = y;

        // get the gcd first
        while (yc != 0) {
            long t = g;
            g = yc;
            yc = t % yc;
        }

        return (int) (x * y / g);
    }

    /**
     * Given a pointer-to-pointer <tt>memptr</tt>, sets the dereferenced value to point to the start
     * of an allocated block of <tt>size</tt> bytes, where the starting address is a multiple of
     * <tt>alignment</tt>. It is guaranteed that the block may be freed by calling @{link {@link #free(Pointer)}
     * on the starting address. See "man 3 posix_memalign".
     *
     * @param memptr    The pointer-to-pointer which will point to the address of the allocated aligned block
     * @param alignment The alignment multiple of the starting address of the allocated block
     * @param size      The number of bytes to allocate
     * @return 0 on success, one of the C error codes on failure.
     */
    public static native int posix_memalign(PointerByReference memptr, NativeLong alignment, NativeLong size);

    // -- alignment logic utility methods

    /**
     * See "man 3 free".
     *
     * @param ptr The pointer to the hunk of memory which needs freeing
     */
    public static native void free(Pointer ptr);

    public static native int ftruncate(int fd, long length);

    private static native NativeLong pwrite(int fd, Pointer buf, NativeLong count, NativeLong offset);

    private static native NativeLong pread(int fd, Pointer buf, NativeLong count, NativeLong offset);

    private static native int open(String pathname, int flags);

    private static native int open(String pathname, int flags, int mode);

    private static native int getpagesize();

    private static native int pathconf(String path, int name);

    private static native String strerror(int errnum);

    /**
     * Interface into native pread function.
     *
     * @param fd     A file descriptor to pass to native pread
     * @param buf    The direct buffer into which to record the file read
     * @param offset The file offset at which to read
     * @return The number of bytes successfully read from the file
     * @throws IOException
     */
    public int pread(int fd, ByteBuffer buf, long offset) throws IOException {
        final int start = buf.position();
        assert start == blockStart(start);
        final int toRead = buf.remaining();
        assert toRead == blockStart(toRead);
        assert offset == blockStart(offset);

        final long address = PlatformDependent.directBufferAddress(buf);
        Pointer pointer = new Pointer(address);
        int n = pread(fd, pointer.share(start), new NativeLong(toRead), new NativeLong(offset)).intValue();
        if (n < 0) {
            throw new IOException("error reading file at offset " + offset + ": " + getLastError());
        }
        buf.position(n);
        return n;
    }

    /**
     * Interface into native pwrite function. Writes bytes corresponding to the nearest file
     * system block boundaries between <tt>buf.position()</tt> and <tt>buf.limit()</tt>.
     *
     * @param fd     A file descriptor to pass to native pwrite
     * @param buf    The direct buffer from which to write
     * @param offset The file offset at which to write
     * @return The number of bytes successfully written to the file
     * @throws IOException
     */
    public int pwrite(int fd, ByteBuffer buf, long offset) throws IOException {

        // must always write to end of current block
        // To handle writes past the logical file size,
        // we will later truncate.
        final int start = buf.position();
        assert start == blockStart(start);
        final int toWrite = buf.remaining();
        assert toWrite == blockStart(toWrite);
        assert offset == blockStart(offset);

        final long address = PlatformDependent.directBufferAddress(buf);
        Pointer pointer = new Pointer(address);

        int n = pwrite(fd, pointer.share(start), new NativeLong(toWrite), new NativeLong(offset)).intValue();
        if (n < 0) {
            throw new IOException("error writing file at offset " + offset + ": " + getLastError());
        }
        buf.position(start + n);
        return n;
    }

    // -- more native function hooks --

    /**
     * Use the <tt>open</tt> Linux system call and pass in the <tt>O_DIRECT</tt> flag.
     * Currently the only other flags passed in are <tt>O_RDONLY</tt> if <tt>readOnly</tt>
     * is <tt>true</tt>, and (if not) <tt>O_RDWR</tt> and <tt>O_CREAT</tt>.
     *
     * @param pathname The path to the file to open. If file does not exist and we are opening
     *                 with <tt>readOnly</tt>, this will throw an error. Otherwise, if it does
     *                 not exist but we have <tt>readOnly</tt> set to false, create the file.
     * @param readOnly Whether to pass in <tt>O_RDONLY</tt>
     * @return An integer file descriptor for the opened file
     */
    public int oDirectOpen(String pathname, boolean readOnly) throws IOException {
        int flags = OpenFlags.INSTANCE.oDIRECT();
        if (readOnly) {
            flags |= OpenFlags.INSTANCE.oRDONLY();
        } else {
            flags |= OpenFlags.INSTANCE.oRDWR() | OpenFlags.INSTANCE.oCREAT();
        }
        int fd = open(pathname, flags, 00644);
        if (fd < 0) {
            throw new IOException("Error opening " + pathname + ", got " + getLastError());
        }
        return fd;
    }

    /**
     * @return The soft block size for use with transfer multiples
     * and memory alignment multiples
     */
    public int blockSize() {
        return fsBlockSize;
    }

    /**
     * Returns the default buffer size for file channels doing O_DIRECT
     * I/O. By default this is equal to the block size.
     *
     * @return The default buffer size
     */
    public int defaultBufferSize() {
        return fsBlockSize;
    }

    /**
     * Given <tt>value</tt>, find the largest number less than or equal
     * to <tt>value</tt> which is a multiple of the fs block size.
     *
     * @param value
     * @return The largest number less than or equal to <tt>value</tt>
     * which is a multiple of the soft block size
     */
    public long blockStart(long value) {
        return value & fsBlockNotMask;
    }

    /**
     * @see #blockStart(long)
     */
    public int blockStart(int value) {
        return (int) (value & fsBlockNotMask);
    }

    /**
     * Given <tt>value</tt>, find the smallest number greater than or equal
     * to <tt>value</tt> which is a multiple of the fs block size.
     *
     * @param value
     * @return The smallest number greater than or equal to <tt>value</tt>
     * which is a multiple of the soft block size
     */
    public long blockEnd(long value) {
        return (value + fsBlockSize - 1) & fsBlockNotMask;
    }

    /**
     * @see #blockEnd(long)
     */
    public int blockEnd(int value) {
        return (int) ((value + fsBlockSize - 1) & fsBlockNotMask);
    }

    /**
     * See "man 2 close"
     *
     * @param fd The file descriptor of the file to close
     * @return 0 on success, -1 on error
     */
    public native int close(int fd); // mustn't forget to do this

}
