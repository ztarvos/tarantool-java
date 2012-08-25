package org.tarantool.core.proto;

public interface Flags {
	/**
	 * DML Request flags
	 */
	static final int RETURN_TUPLE = 0x01;
	static final int ADD_TUPLE = 0x02;
	static final int REPLACE_TUPLE = 0x04;

}
