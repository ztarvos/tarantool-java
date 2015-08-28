package org.tarantool;

import java.io.IOException;
import java.lang.reflect.Field;
import java.nio.ByteBuffer;
import java.nio.channels.SocketChannel;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.tarantool.schema.IndexId;
import org.tarantool.schema.Space;
import org.tarantool.schema.SpaceId;

public abstract class TarantoolConnection16Base {
    protected final SocketChannel channel;
    protected final ConnectionState state;
    protected final String salt;


    protected SocketChannel getChannel() {
        return channel;
    }

    protected ConnectionState getState() {
        return state;
    }

    public TarantoolConnection16Base(SocketChannel channel) {
        try {
            this.channel = channel;
            this.state = new ConnectionState();
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

    protected int readFully(ByteBuffer buffer) {
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

    protected Object read() {
        readPacket();
        return state.getBody().get(Key.DATA);
    }

    protected void readPacket() {
        readFully(state.getLengthReadBuffer());
        readFully(state.getPacketReadBuffer());
        state.unpack();
        long code = (Long) state.getHeader().get(Key.CODE);
        if (code != 0) {
            Object error = state.getBody().get(Key.ERROR);
            throw new TarantoolException((int) code, error instanceof String ? (String) error : new String((byte[]) error));
        }
    }

    protected int write(ByteBuffer buffer) {
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

    public List select(int space, int index, Object key, int offset, int limit, int iterator) {
        return exec(Code.SELECT, Key.SPACE, space, Key.INDEX, index, Key.KEY, key, Key.ITERATOR, iterator, Key.LIMIT, limit, Key.OFFSET, offset);
    }

    public List insert(int space, Object tuple) {
        return exec(Code.INSERT, Key.SPACE, space, Key.TUPLE, tuple);
    }

    public List replace(int space, Object tuple) {
        return exec(Code.REPLACE, Key.SPACE, space, Key.TUPLE, tuple);
    }

    public List update(int space, Object key, Object... args) {
        return exec(Code.UPDATE, Key.SPACE, space, Key.KEY, key, Key.TUPLE, args);
    }

    public void upsert(int space, Object key, Object def,Object... args) {
        exec(Code.UPSERT, Key.SPACE, space, Key.KEY, key, Key.TUPLE, def, Key.UPSERT_OPS, args);
    }

    public List delete(int space, Object key) {
        return exec(Code.DELETE, Key.SPACE, space, Key.KEY, key);
    }

    public List call(String function, Object... args) {
        return exec(Code.CALL, Key.FUNCTION, function, Key.TUPLE, args);
    }

    public List eval(String expression, Object... args) {
        return exec(Code.EVAL, Key.EXPRESSION, expression, Key.TUPLE, args);
    }



    public void auth(String username, final String password) {
        try {
            final MessageDigest sha1 = MessageDigest.getInstance("SHA-1");
            List auth = new ArrayList(2);
            auth.add("chap-sha1");

            byte[] p = sha1.digest(password.getBytes());

            sha1.reset();
            byte[] p2 = sha1.digest(p);

            sha1.reset();
            sha1.update(Base64.decode(salt), 0, 20);
            sha1.update(p2);
            byte[] scramble = sha1.digest();
            for (int i = 0, e = 20; i < e; i++) {
                p[i] ^= scramble[i];
            }
            auth.add(p);
            exec(Code.AUTH, Key.USER_NAME, username, Key.TUPLE, auth);

        } catch (NoSuchAlgorithmException e) {
            throw new CommunicationException("Can't use sha-1", e);
        }
    }

    public void ping() {
        exec(Code.PING);
    }

    public void close() {
        try {
            channel.close();
        } catch (IOException ignored) {

        }
    }

    public <T> T schema(T schema)  {
        final Map<String, Integer> spaces = callMap(281, new int[]{2}, 0, "");
        final String idxSep = "_";
        final Map<String, Integer> indexes = callMap(289, new int[]{0, 2}, 1, idxSep);
        final Field[] fields = schema.getClass().getFields();
        for (Field field : fields) {
            final Space space = field.getAnnotation(Space.class);
            if (space != null) {
                String spaceName = space.value().isEmpty() ? field.getName() : space.value();
                final Integer spaceIndex = spaces.get(spaceName);
                if(spaceIndex == null) {
                    throw new IllegalStateException("Can't find ID for space "+spaceName);
                }
                try {
                    final Object spaceObject = field.get(schema);
                    for (Field f : spaceObject.getClass().getFields()) {
                        final SpaceId spaceId = f.getAnnotation(SpaceId.class);
                        final IndexId indexId = f.getAnnotation(IndexId.class);
                        if (spaceId != null) {
                            f.set(spaceObject, f.getClass().isPrimitive() ? spaceIndex.intValue() : spaceIndex);
                        } else if(indexId!=null) {
                            final String indexName = indexId.value().isEmpty() ? f.getName() : indexId.value();
                            final Integer indexIdx = indexes.get(spaceIndex + idxSep + indexName);
                            if(indexIdx == null) {
                                throw new IllegalStateException("Can't find index id " + spaceName + "." + indexName);
                            }
                            f.set(spaceObject, indexIdx);
                        }
                    }
                } catch (IllegalAccessException e) {
                    throw new IllegalStateException("All schema field should be accessible", e);
                }
            }
        }
        return schema;
    }

    protected <K, V> Map<K, V> callMap(int space, int[] key, int value, String keySeparator, Object... args) {
        final List<List> tuples = select(space, 0, Arrays.asList(), 0, 1000, 0);
        Map result = new HashMap();
        for (List tuple : tuples) {
            StringBuilder keyValue = new StringBuilder();
            for (Integer k : key) {
                if (keyValue.length() > 0) {
                    keyValue.append(keySeparator);
                }
                keyValue.append(tuple.get(k));
            }
            result.put(keyValue.toString(), tuple.get(value));
        }
        return result;
    }


    protected abstract List exec(Code code, Object... args);
}
