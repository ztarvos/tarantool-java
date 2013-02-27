package org.tarantool.core.proto;

import java.nio.ByteBuffer;
import java.util.Arrays;


public class Leb128 {
    /**
     * <p>
     * readUnsigned.
     * </p>
     *
     * @param buffer
     *            a {@link java.nio.ByteBuffer} object.
     * @return a int.
     */
    public static int readUnsigned(ByteBuffer buffer) {
        int result = 0;
        int cur;
        int count = 0;
        do {
            cur = buffer.get() & 0xff;
            result <<=count * 7;
            result |= (cur & 0x7f);
            count++;
        } while (((cur & 0x80) == 0x80));

        if ((cur & 0x80) == 0x80) {
            throw new IllegalArgumentException("Can't read LEB128 from buffer");
        }

        return result;
    }

    /**
     * <p>
     * unsignedSize.
     * </p>
     *
     * @param value
     *            a int.
     * @return a int.
     */
    public static int unsignedSize(int value) {
        int remaining = value >> 7;
        int count = 0;
        while (remaining != 0) {
            remaining >>= 7;
            count++;
        }
        return count + 1;
    }

    /**
     * <p>
     * writeUnsigned.
     * </p>
     *
     * @param buffer
     *            a {@link java.nio.ByteBuffer} object.
     * @param value
     *            a int.
     * @return a {@link java.nio.ByteBuffer} object.
     */
    public static ByteBuffer writeUnsigned(ByteBuffer buffer, int value) {
        int remaining = value >>> 7;
        int[] bts=new int[unsignedSize(value)];
        int p=0;
        while (remaining != 0) {
            //buffer.put((byte) ((value & 0x7f) | 0x80));
            bts[p++] = (value & 0x7f);
            value = remaining;
            remaining >>>= 7;
        }
        bts[p]=value;
        for(int i=bts.length-1;i>0;i--) {
            buffer.put((byte) ((bts[i] & 0x7f) | 0x80));
        }
        buffer.put((byte) (bts[0] & 0x7f));
        return buffer;
    }

   
}
