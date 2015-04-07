package org.tarantool;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.SocketChannel;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.List;


public class TarantoolConnection16Impl extends TarantoolConnection16Base implements TarantoolConnection16 {

    public TarantoolConnection16Impl(SocketChannel channel) {
        super(channel);
    }

    public TarantoolConnection16Impl(String host, int port) throws IOException {
        this(SocketChannel.open(new InetSocketAddress(host, port)));
    }


    protected List exec(Code code, Object... args) {
        write(state.pack(code, args));
        return (List) read();
    }


}
