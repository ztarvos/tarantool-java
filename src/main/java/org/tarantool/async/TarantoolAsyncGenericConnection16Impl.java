package org.tarantool.async;

import java.nio.channels.SocketChannel;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import org.tarantool.generic.Mapper;

public class TarantoolAsyncGenericConnection16Impl extends TarantoolAsyncConnection16Impl implements TarantoolAsyncGenericConnection16 {
    private final Mapper mapper;

    public TarantoolAsyncGenericConnection16Impl(Mapper mapper, TarantoolSelectorWorker worker, SocketChannel channel, String username, String password, long timeout, TimeUnit unit) {
        super(worker, channel, username, password, timeout, unit);
        this.mapper = mapper;
    }

    @Override
    public <T> Future<T> select(Class<T> clz, int space, int index, Object key, int offset, int limit, int iterator) {
        return new AsyncGenericQuery<T>(mapper, clz, super.select(space, index, key, offset, limit, iterator));
    }

    @Override
    public <T> Future<T> insert(Class<T> clz, int space, Object tuple) {
        return new AsyncGenericQuery<T>(mapper, clz, super.insert(space, mapper.toTuple(tuple)));
    }

    @Override
    public <T> Future<T> replace(Class<T> clz, int space, Object tuple) {
        return new AsyncGenericQuery<T>(mapper, clz, super.replace(space, mapper.toTuple(tuple)));
    }

    @Override
    public <T> Future<T> update(Class<T> clz, int space, Object key, Object... args) {
        return new AsyncGenericQuery<T>(mapper, clz, super.update(space, key, mapper.toTuples(args)));
    }

    @Override
    public <T> Future<T> delete(Class<T> clz, int space, Object key) {
        return new AsyncGenericQuery<T>(mapper, clz, super.delete(space, key));
    }

    @Override
    public <T> Future<T> call(Class<T> clz, String function, Object... args) {
        return new AsyncGenericQuery<T>(mapper, clz, super.call(function, mapper.toTuples(args)));
    }

    @Override
    public <T> Future<T> eval(Class<T> clz, String expression, Object... args) {
        return new AsyncGenericQuery<T>(mapper, clz, super.eval(expression, mapper.toTuples(args)));
    }
}
