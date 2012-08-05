package org.tarantool.snapshot;

public class Const {
	public static final byte[] HEADER = "SNAP\n0.11\n\n".getBytes();
	public static final short SNAP_TAG = (short) 0xFFFF;
	public static final int ROW_HEADER_SIZE = 32;
	public static final int DATA_HEADER_SIZE = 22;
	public static final int ROW_START_MARKER = 0xba0babed;
	public static final int EOF_MARKER = 0x10adab1e;
	
}
