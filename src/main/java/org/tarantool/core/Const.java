package org.tarantool.core;

/**
 * Constant values for Tarantool protocol.
 */
public interface Const {

	/**
	 * Request types
	 */
	public enum OP {

		INSERT(13), SELECT(17), UPDATE(19), DELETE(21), CALL(22), PING(65280);
		private OP(int type) {
			this.type = type;
		}

		public static OP valueOf(int type) {
			for (OP op : OP.values()) {
				if (op.type == type) {
					return op;
				}
			}
			return null;
		}

		public int type;
	}

	/**
	 * DML Request flags
	 */
	static final int RETURN_TUPLE = 0x01;
	static final int ADD_TUPLE = 0x02;
	static final int REPLACE_TUPLE = 0x04;

	/**
	 * Update operations
	 */
	public enum UP {
		SET(0, 1), ADD(1, 1), AND(2, 1), XOR(3, 1), OR(4, 1), SPLICE(5, 3), DELETE(6, 1), INSERT(7, 1), SUB(8, 1), NONE(9, 1), MAX(10, 1);
		private UP(int type, int args) {
			this.type = type;
			this.args = args;
		}

		public static UP valueOf(int type) {
			for (UP op : UP.values()) {
				if (op.type == type) {
					return op;
				}
			}
			return null;
		}

		public int type;
		public int args;

	}

}
