package org.tarantool.msgpack;


import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.IdentityHashMap;
import java.util.Map;

import org.msgpack.packer.MessagePackPacker;
import org.msgpack.template.AnyTemplate;
import org.msgpack.template.CustomAnyTemplate;
import org.msgpack.template.EnumTemplate;
import org.msgpack.template.LessLockMessagePack;
import org.msgpack.template.MapTemplate;
import org.msgpack.unpacker.MessagePackUnpacker;
import org.msgpack.unpacker.Unpacker;

public class ConnetionState {


    protected static final LessLockMessagePack pack = new LessLockMessagePack() {
        {
            getRegistry().register(Code.class, new EnumTemplate(Code.AUTH));
        }
    };

    protected static final EnumTemplate<Key> keyTemplate = new EnumTemplate<>(Key.CODE);
    protected static final AnyTemplate<Object> valueTemplate = new CustomAnyTemplate(pack.getRegistry());


    protected static final MapTemplate<Key, Object> mapTemplate = new MapTemplate<Key, Object>(keyTemplate, valueTemplate) {
        @Override
        public Map<Key, Object> read(Unpacker u, Map<Key, Object> to, boolean required) throws IOException {
            if (!required && u.trySkipNil()) {
                return null;
            }
            int n = u.readMapBegin();
            for (int i = 0; i < n; i++) {
                Key key = keyTemplate.read(u, null);
                Object value;
                Class<?> cls;
                if (key == Key.DATA && (cls = (Class<?>) to.get(Key.DATA)) != null) {
                    value = u.read(cls);
                } else {
                    value = valueTemplate.read(u, null);
                }
                to.put(key, value);
            }
            u.readMapEnd();
            return to;
        }

    };


    protected IdentityHashMap<Key, Object> header = new IdentityHashMap<Key, Object>();
    protected IdentityHashMap<Key, Object> body = new IdentityHashMap<Key, Object>();

    protected ByteBufferStreams buffer = new ByteBufferStreams(ByteBuffer.allocate(32 * 1024), 1.1d);
    protected MessagePackUnpacker packetUnpacker = new MessagePackUnpacker(pack, buffer.asInputStream());
    protected MessagePackPacker packetPacker = new MessagePackPacker(pack, buffer.asOutputStream());


    public ByteBuffer getWelcomeBuffer() {
        ByteBuffer buf = buffer.getBuf();
        buf.clear();
        buf.limit(64);
        return buf;
    }

    public ByteBuffer getLengthReadBuffer() {
        ByteBuffer buf = buffer.getBuf();
        buf.clear();
        buf.limit(5);
        return buf;
    }

    public ByteBuffer getPacketReadBuffer() {
        ByteBuffer buf = buffer.getBuf();
        buf.limit(buf.position());
        buf.rewind();
        try {
            int size = packetUnpacker.readInt();
            buf.clear();
            buffer.checkCapacityAndSetLimit(size);
            return buf;
        } catch (IOException e) {
            //this shouldn't happens
            throw new IllegalStateException(e);
        }
    }

    public void unpack() {
        unpack(null);
    }

    public void unpack(Class<?> dataClass) {
        ByteBuffer buf = buffer.getBuf();
        buf.limit(buf.position());
        buf.rewind();
        body.clear();
        body.put(Key.DATA, dataClass);
        header.clear();
        try {
            mapTemplate.read(packetUnpacker, header);
            if (buf.remaining() > 0) {
                mapTemplate.read(packetUnpacker, body, true);
            }
        } catch (IOException e) {
            //this shouldn't happens
            throw new IllegalStateException(e);
        }
    }


    public ByteBuffer pack(Code code, Object[] args) {
        beginBody();
        if (args != null) {
            for (int i = 0, e = args.length; i < e; i += 2) {
                Object value = args[i + 1];
                put((Key) args[i], value);
            }
        }
        endBody();
        return pack(code, null, body);
    }

    public ByteBuffer pack(Code code, Object sync, IdentityHashMap<Key, Object> body) {
        header.clear();
        header.put(Key.CODE, code);
        if (sync != null) {
            header.put(Key.SYNC, sync);
        }

        ByteBuffer buf = buffer.getBuf();
        buf.clear();
        buf.put((byte) 0xce);
        buf.putInt(0);
        try {
            mapTemplate.write(packetPacker, header);
            mapTemplate.write(packetPacker, body);
        } catch (IOException e) {
            //this shouldn't happens
            throw new IllegalStateException(e);
        }
        buf.putInt(1, buf.position() - 5);
        buf.limit(buf.position());
        buf.rewind();
        return buf;
    }

    public ConnetionState beginBody() {
        body.clear();
        return this;
    }

    public ConnetionState put(Key key, Object value) {
        if (value != null) {
            body.put(key, value);
        }
        return this;
    }

    public IdentityHashMap<Key, Object> endBody() {
        return body;
    }

    public IdentityHashMap<Key, Object> getHeader() {
        return header;
    }

    public IdentityHashMap<Key, Object> getBody() {
        return body;
    }
}
