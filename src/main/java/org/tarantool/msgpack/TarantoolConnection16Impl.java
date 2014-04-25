package org.tarantool.msgpack;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.SocketChannel;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.msgpack.type.ArrayValue;
import org.msgpack.type.IntegerValue;
import org.msgpack.type.RawValue;
import org.msgpack.type.Value;
import org.tarantool.core.exception.CommunicationException;
import org.tarantool.core.exception.TarantoolException;
import org.tarantool.pool.ConnectionReturnPoint;
import org.tarantool.pool.Returnable;

import sun.misc.BASE64Decoder;


public class TarantoolConnection16Impl implements TarantoolConnection16, Returnable {
    private final SocketChannel channel;
    private final ConnetionState state;
    private final String salt;
    private ConnectionReturnPoint<TarantoolConnection16> connectionReturnPoint;


    public TarantoolConnection16Impl(String host, int port) {
        try {
            this.channel = SocketChannel.open(new InetSocketAddress(host, port));
            this.state = new ConnetionState();
            ByteBuffer welcome = state.getWelcomeBuffer();
            readFully(welcome);
            String firstLine = new String(welcome.array(), 0, welcome.position());
            if (!firstLine.startsWith("Tarantool")) {
                channel.close();
                throw new CommunicationException("Welcome message should starts with tarantool but starts with '" + firstLine + "'");
            }
            welcome = state.getWelcomeBuffer();
            readFully(welcome);
            this.salt = new String(welcome.array(), 0, welcome.position());
        } catch (IOException e) {
            throw new CommunicationException("Can't connect with tarantool", e);
        }
    }

    private int readFully(ByteBuffer buffer) {
        try {
            int code;
            while ((code = channel.read(buffer)) > -1 && buffer.remaining() > 0) {

            }
            if (code < 0) {
                throw new CommunicationException("Can't read bytes");
            }
            return code;
        } catch (IOException e) {
            throw new CommunicationException("Can't read bytes", e);
        }
    }

    private ArrayValue exec(Code code, Object... args) {
        try {
            write(state.pack(code, args));
            return (ArrayValue) read();
        } finally {
            if (connectionReturnPoint != null) {
                connectionReturnPoint.returnConnection(this);
            }
        }
    }

    private <R> R exec(Class<R> cls, Code code, Object... args) {
        try {
            write(state.pack(code, args));
            return (R) read(cls);
        } finally {
            if (connectionReturnPoint != null) {
                connectionReturnPoint.returnConnection(this);
            }
        }
    }


    private Object read() {
        return read(null);
    }

    private Object read(Class<?> cls) {
        readFully(state.getLengthReadBuffer());
        readFully(state.getPacketReadBuffer());
        state.unpack(cls);
        IntegerValue code = (IntegerValue) state.getHeader().get(Key.CODE);
        if (code.getInt() != 0) {
            RawValue error = (RawValue) state.getBody().get(Key.ERROR);
            throw new TarantoolException(code.getInt(), error.getString());
        }
        return state.getBody().get(Key.DATA);
    }

    private int write(ByteBuffer buffer) {
        try {
            int code;
            while ((code = channel.write(buffer)) > -1 && buffer.remaining() > 0) {

            }
            if (code < 0) {
                throw new CommunicationException("Can't read bytes");
            }
            return code;
        } catch (IOException e) {
            throw new CommunicationException("Can't write bytes", e);
        }

    }


    @Override
    public <R> R select(Class<R> cls, int space, int index, Object key, int offset, int limit) {
        return exec(cls, Code.SELECT, Key.SPACE, space, Key.INDEX, index, Key.KEY, key, Key.OFFSET, offset, Key.LIMIT, limit);
    }

    @Override
    public List<Value> select(int space, int index, Object key, int offset, int limit) {
        return exec(Code.SELECT, Key.SPACE, space, Key.INDEX, index, Key.KEY, key);
    }

    @Override
    public <R> R insert(Class<R> cls, int space, Object tuple) {
        return exec(cls, Code.INSERT, Key.SPACE, space, Key.TUPLE, tuple);
    }

    @Override
    public List<Value> insert(int space, Object tuple) {
        return exec(Code.INSERT, Key.SPACE, space, Key.TUPLE, tuple);
    }

    @Override
    public List<Value> replace(int space, Object tuple) {
        return exec(Code.REPLACE, Key.SPACE, space, Key.TUPLE, tuple);
    }

    @Override
    public <R> R replace(Class<R> cls, int space, Object tuple) {
        return exec(cls, Code.REPLACE, Key.SPACE, space, Key.TUPLE, tuple);
    }

    @Override
    public List<Value> update(int space, Object key, Object tuple) {
        return exec(Code.UPDATE, Key.SPACE, space, Key.KEY, key, Key.TUPLE, tuple);
    }

    @Override
    public <R> R update(Class<R> cls, int space, Object key, Object tuple) {
        return exec(cls, Code.UPDATE, Key.SPACE, space, Key.KEY, key, Key.TUPLE, tuple);
    }

    @Override
    public List<Value> delete(int space, Object key) {
        return exec(Code.DELETE, Key.SPACE, space, Key.KEY, key);
    }

    @Override
    public <R> R delete(Class<R> cls, int space, Object key) {
        return exec(cls, Code.DELETE, Key.SPACE, space, Key.KEY, key);
    }

    @Override
    public List<Value> call(String function, Object... args) {
        return exec(Code.CALL, Key.FUNCTION, function, Key.TUPLE, Arrays.asList(args));
    }

    @Override
    public <R> R call(Class<R> cls, String function, Object... args) {
        return exec(cls, Code.CALL, Key.FUNCTION, function, Key.TUPLE, Arrays.asList(args));
    }

    @Override
    public void auth(String username, final String password) {
        try {
            final MessageDigest sha1 = MessageDigest.getInstance("SHA-1");
            List auth = new ArrayList(2);
            auth.add("chap-sha1");

            byte[] p = sha1.digest(password.getBytes());

            sha1.reset();
            byte[] p2 = sha1.digest(p);

            sha1.reset();
            BASE64Decoder decoder = new BASE64Decoder();
            sha1.update(decoder.decodeBuffer(salt), 0, 20);
            sha1.update(p2);
            byte[] scramble = sha1.digest();
            for (int i = 0, e = 20; i < e; i++) {
                p[i] ^= scramble[i];
            }
            auth.add(p);
            exec(Code.AUTH, Key.USER_NAME, username, Key.TUPLE, auth);

        } catch (NoSuchAlgorithmException e) {
            throw new CommunicationException("Can't use sha-1", e);
        } catch (IOException e) {
            throw new CommunicationException("Can't decode base-64", e);

        }
    }


    @Override
    public Boolean ping() {
        exec(Code.PING);
        return true;
    }

    @Override
    public void close() {
        try {
            channel.close();
        } catch (IOException ignored) {

        }
    }

    @Override
    public void returnTo(ConnectionReturnPoint returnPoint) {
        connectionReturnPoint = returnPoint;
    }
}
