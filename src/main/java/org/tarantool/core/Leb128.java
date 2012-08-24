package org.tarantool.core;

import java.nio.ByteBuffer;

/**
 * Leb128 encoder/decoder
 */
public class Leb128 {

	public static int readUnsigned(ByteBuffer buffer) {
		int result = 0;
		int cur;
		int count = 0;

		do {
			cur = buffer.get() & 0xff;
			result |= (cur & 0x7f) << (count * 7);
			count++;
		} while (((cur & 0x80) == 0x80) && count < 5);

		if ((cur & 0x80) == 0x80) {
			throw new IllegalArgumentException("Can't read LEB128 from buffer");
		}

		return result;
	}

	public static int unsignedSize(int value) {
		int remaining = value >> 7;
		int count = 0;
		while (remaining != 0) {
			remaining >>= 7;
			count++;
		}
		return count + 1;
	}

	public static ByteBuffer writeUnsigned(ByteBuffer buffer, int value) {
		int remaining = value >>> 7;

		while (remaining != 0) {
			buffer.put((byte) ((value & 0x7f) | 0x80));
			value = remaining;
			remaining >>>= 7;
		}

		buffer.put((byte) (value & 0x7f));
		return buffer;
	}
}
