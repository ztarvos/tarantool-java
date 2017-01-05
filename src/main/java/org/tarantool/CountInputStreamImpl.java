package org.tarantool;

import java.io.IOException;
import java.io.InputStream;

public class CountInputStreamImpl extends CountInputStream {

    protected InputStream is;
    protected long bytesRead;

    public CountInputStreamImpl(InputStream is) throws IOException {
        this.is = is;
    }

    @Override
    public int read() throws IOException {
        bytesRead++;
        return 0XFF & is.read();
    }

    @Override
    public int read(byte[] b, final int off, final int len) throws IOException {
        int read = is.read(b, off, len);
        bytesRead += read;
        return read;
    }

    @Override
    public void close() throws IOException {
        is.close();
    }

    public long getBytesRead() {
        return bytesRead;
    }
}
