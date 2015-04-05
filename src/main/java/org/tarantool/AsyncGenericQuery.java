package org.tarantool;

import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

public class AsyncGenericQuery<T> implements Future<T> {
    Future<List> delegate;
    Mapper mapper;
    Class<T> cls;

    public AsyncGenericQuery(Mapper mapper, Class<T> cls, Future<List> delegate) {
        this.delegate = delegate;
        this.mapper = mapper;
        this.cls = cls;
    }

    @Override
    public boolean cancel(boolean mayInterruptIfRunning) {
        return delegate.cancel(mayInterruptIfRunning);
    }

    @Override
    public boolean isCancelled() {
        return delegate.isCancelled();
    }

    @Override
    public boolean isDone() {
        return delegate.isDone();
    }

    @Override
    public T get() throws InterruptedException, ExecutionException {
        List list = delegate.get();
        return mapper.toObject(cls, list);
    }

    @Override
    public T get(long timeout, TimeUnit unit) throws InterruptedException, ExecutionException, TimeoutException {
        List list = delegate.get(timeout, unit);
        return mapper.toObject(cls, list);
    }
}
