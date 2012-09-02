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
	/** Constant <code>HEADER="SNAP\n0.11\n\n".getBytes()</code> */
	public static final byte[] HEADER = "SNAP\n0.11\n\n".getBytes();
	/** Constant <code>SNAP_TAG=(short) 0xFFFF</code> */
	public static final short SNAP_TAG = (short) 0xFFFF;
	/** Constant <code>ROW_HEADER_SIZE=32</code> */
	public static final int ROW_HEADER_SIZE = 32;
	/** Constant <code>DATA_HEADER_SIZE=22</code> */
	public static final int DATA_HEADER_SIZE = 22;
	/** Constant <code>ROW_START_MARKER=0xba0babed</code> */
	public static final int ROW_START_MARKER = 0xba0babed;
	/** Constant <code>EOF_MARKER=0x10adab1e</code> */
	public static final int EOF_MARKER = 0x10adab1e;

}
