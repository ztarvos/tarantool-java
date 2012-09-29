package org.tarantool.snapshot;

/**
 * <p>
 * Const class.
 * </p>
 * 
 * @author dgreen
 * @version $Id: $
 */
public class Const {
	public static final byte[] SNAP_HEADER = "SNAP\n0.11\n\n".getBytes();
	public static final byte[] XLOG_HEADER = "XLOG\n0.11\n\n".getBytes();
	public static final short SNAP_TAG = (short) 0xFFFF;
	public static final short XLOG_TAG = (short) 0xFFFE;
	public static final int HEADER_SIZE = 28;
	public static final int ROW_DATA_HEADER_SIZE = 22;
	public static final int ROW_START_MARKER = 0xba0babed;
	public static final int EOF_MARKER = 0x10adab1e;
	public static final int VERSION = 11;

}
