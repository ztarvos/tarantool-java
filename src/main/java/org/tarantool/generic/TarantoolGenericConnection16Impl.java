package org.tarantool.generic;


import java.io.IOException;
import java.nio.channels.SocketChannel;

import org.tarantool.TarantoolConnection16Impl;

public class TarantoolGenericConnection16Impl extends TarantoolConnection16Impl implements TarantoolGenericConnection16 {
    private final Mapper mapper;

    public TarantoolGenericConnection16Impl(String host, int port, Mapper mapper) throws IOException {
        super(host, port);
        this.mapper = mapper;
    }

    public TarantoolGenericConnection16Impl(SocketChannel channel, Mapper mapper) {
        super(channel);
        this.mapper = mapper;
    }

    @Override
    public <T> T select(Class<T> clz, int space, int index, Object key, int offset, int limit, int iterator) {
        return mapper.toObject(clz, super.select(space, index, key, offset, limit, iterator));
    }

    @Override
    public <T> T insert(Class<T> clz, int space, Object tuple) {
        return mapper.toObject(clz, super.insert(space, mapper.toTuple(tuple)));
    }

    @Override
    public <T> T replace(Class<T> clz, int space, Object tuple) {
        return mapper.toObject(clz, super.replace(space, mapper.toTuple(tuple)));
    }

    @Override
    public <T> T update(Class<T> clz, int space, Object key, Object...args) {
        return mapper.toObject(clz, super.update(space, key, mapper.toTuples(args)));
    }

    @Override
    public <T> void upsert(Class<T> clz, int space, Object key, Object def, Object... args) {
        super.upsert(space, key, mapper.toTuple(def), mapper.toTuples(args));
    }

    @Override
    public <T> T delete(Class<T> clz, int space, Object key) {
        return mapper.toObject(clz, super.delete(space, key));
    }

    @Override
    public <T> T call(Class<T> clz, String function, Object... args) {
        return mapper.toObject(clz, super.call(function, mapper.toTuples(args)));
    }

    @Override
    public <T> T eval(Class<T> clz, String expression, Object... args) {
        return mapper.toObject(clz, super.eval(expression, mapper.toTuples(args)));
    }


}
