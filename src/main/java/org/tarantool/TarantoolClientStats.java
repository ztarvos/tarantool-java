package org.tarantool;

public class TarantoolClientStats {
    final long start = System.currentTimeMillis();
    public long buffered;
    public long received;
    public long sharedWrites;
    public long directWrite;
    public long directMaxPacketSize;
    public long sharedMaxPacketSize;
    public long directPacketSizeGrowth;
    public long sharedPacketSizeGrowth;
    public long sharedEmptyAwait;
    public long sharedWriteLockTimeouts;
    public long directWriteLockTimeouts;
    public long sharedEmptyAwaitTimeouts;

    @Override
    public String toString() {
        return "TarantoolClientStats" +
                "\nrunning = " + (System.currentTimeMillis() - start) + "ms" +
                "\nbuffered = " + buffered +
                "\nreceived = " + received +
                "\ndirectMaxPacketSize = " + directMaxPacketSize +
                "\nsharedMaxPacketSize = " + sharedMaxPacketSize +
                "\nsharedEmptyAwait = " + sharedEmptyAwait +
                "\nsharedEmptyAwaitTimeouts = " + sharedEmptyAwaitTimeouts +
                "\ndirectMaxPacketSizeGrowth = " + directPacketSizeGrowth +
                "\nsharedMaxPacketSizeGrowth = " + sharedPacketSizeGrowth +
                "\ndirectWriteLockTimeouts = " + directWriteLockTimeouts +
                "\nsharedWriteLockTimeouts = " + sharedWriteLockTimeouts +
                "\ndirectWrite = " + directWrite +
                "\nsharedWrites = " + sharedWrites + "\n";
    }
}
