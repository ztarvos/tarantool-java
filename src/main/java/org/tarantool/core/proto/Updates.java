package org.tarantool.core.proto;

/**
 * Update operations
 *
 * @author dgreen
 * @version $Id: $
 */
public enum Updates {
	SET(0, 1), ADD(1, 1), AND(2, 1), XOR(3, 1), OR(4, 1), SPLICE(5, 3), DELETE(6, 1), INSERT(7, 1), SUB(8, 1), NONE(9, 1), MAX(10, 1);
	/**
	 * <p>Constructor for Updates.</p>
	 *
	 * @param type a int.
	 * @param args a int.
	 */
	private Updates(int type, int args) {
		this.type = type;
		this.args = args;
	}

	/**
	 * <p>valueOf.</p>
	 *
	 * @param type a int.
	 * @return a {@link org.tarantool.core.proto.Updates} object.
	 */
	public static Updates valueOf(int type) {
		for (Updates op : Updates.values()) {
			if (op.type == type) {
				return op;
			}
		}
		return null;
	}

	public int type;
	public int args;

}
