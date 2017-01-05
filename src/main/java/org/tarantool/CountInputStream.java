package org.tarantool;

import java.io.InputStream;

public abstract class CountInputStream extends InputStream {
    abstract long getBytesRead();
}
