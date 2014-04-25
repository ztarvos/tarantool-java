package org.tarantool.msgpack;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.ByteBuffer;

public class ByteBufferStreams {

    protected ByteBuffer buf;
    protected double resizeFactor = 1.1;
    private OutputStream os = new OutputStream() {
        @Override
        public void write(int b) throws IOException {
            buf.put((byte) b);
        }

        @Override
        public void write(byte[] b, int off, int len) throws IOException {
            if (len > buf.remaining()) {
                checkCapacityAndSetLimit(buf.capacity() + len - buf.remaining());
            }
            buf.put(b, off, len);
        }
    };

    private InputStream is = new InputStream() {
        @Override
        public int read() throws IOException {
            return 0xFF & buf.get();
        }

        @Override
        public int read(byte[] bytes, int off, int len)
                throws IOException {
            if (!buf.hasRemaining()) {
                return -1;
            }

            len = Math.min(len, buf.remaining());
            buf.get(bytes, off, len);
            return len;
        }

    };

    public ByteBufferStreams(ByteBuffer buf, double resizeFactor) {
        this.buf = buf;
        this.resizeFactor = resizeFactor;
    }


    public void checkCapacityAndSetLimit(int size) {
        if (buf.capacity() < size) {
            ByteBuffer newBuf = ByteBuffer.allocate(size);
            buf.rewind();
            newBuf.put(buf);
            buf = newBuf;
        }
        buf.limit(size);
    }





    public OutputStream asOutputStream() {
        return os;
    }

    public InputStream asInputStream() {
        return is;
    }

    public void clear() {
        buf.clear();
    }

    public ByteBuffer getBuf() {
        return buf;
    }
}