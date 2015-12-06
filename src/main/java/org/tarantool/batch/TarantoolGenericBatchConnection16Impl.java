package org.tarantool.batch;

import java.nio.channels.SocketChannel;

import org.tarantool.generic.Mapper;

public class TarantoolGenericBatchConnection16Impl extends TarantoolBatchConnection16Impl implements TarantoolGenericBatchConnection16 {

    protected final Mapper mapper;

    public TarantoolGenericBatchConnection16Impl(SocketChannel channel, Mapper mapper) {
        super(channel);
        this.mapper = mapper;
    }

    protected <T> Holder<T> wrap(final Class<T> clz, final BatchedQueryResult result) {
        return new Holder<T>() {
            @Override
            public T get()  {
                if (batch != null) {
                    throw new IllegalStateException("You should end batch first");
                }
                if (result.getError() != null) {
                    throw result.getError();
                }
                return mapper.toObject(clz, result.getResult());
            }
        };
    }

    @Override
    public <T> Holder<T> select(final Class<T> clz, int space, int index, Object key, int offset, int limit, int iterator) {
        return wrap(clz, super.select(space, index, key, offset, limit, iterator));
    }

    @Override
    public <T> Holder<T> insert(Class<T> clz, int space, Object tuple) {
        return wrap(clz, super.insert(space, mapper.toTuple(tuple)));
    }

    @Override
    public <T> Holder<T> replace(Class<T> clz, int space, Object tuple) {
        return wrap(clz, super.replace(space, mapper.toTuple(tuple)));
    }

    @Override
    public <T> Holder<T> update(Class<T> clz, int space, Object key, Object... args) {
        return wrap(clz, super.update(space, key, mapper.toTuples(args)));
    }

    @Override
    public <T> Holder<T> delete(Class<T> clz, int space, Object key) {
        return wrap(clz, super.delete(space, key));
    }

    @Override
    public <T> Holder<T> call(Class<T> clz, String function, Object... args) {
        return wrap(clz, super.call(function, mapper.toTuples(args)));
    }

    @Override
    public <T> Holder<T> eval(Class<T> clz, String expression, Object... args) {
        return wrap(clz, super.eval(expression, mapper.toTuples(args)));
    }

}
