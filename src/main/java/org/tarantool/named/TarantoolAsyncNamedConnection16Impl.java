package org.tarantool.named;

import java.nio.channels.SocketChannel;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.Future;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;

import org.tarantool.Code;
import org.tarantool.CommunicationException;
import org.tarantool.Key;
import org.tarantool.TarantoolException;
import org.tarantool.async.AsyncQuery;
import org.tarantool.async.TarantoolAsyncConnection16Impl;
import org.tarantool.async.TarantoolSelectorWorker;

public class TarantoolAsyncNamedConnection16Impl extends TarantoolNamedBase16<Future<List>> implements TarantoolAsyncNamedConnection16{
    protected TarantoolAsyncConnection16Impl delegate;
    protected Future<List> spaces;
    protected Future<List> indexes;
    protected Semaphore schemaUpdate = new Semaphore(1);

    public TarantoolAsyncNamedConnection16Impl(TarantoolSelectorWorker worker, SocketChannel channel, String username, String password, long timeout, TimeUnit unit) {
        delegate = new TarantoolAsyncConnection16Impl(worker, channel, username, password, timeout, unit) {

            @Override
            public void auth(String username, String password) {
                super.auth(username, password);
                try {
                    buildSchema(select(VSPACE, 0, Collections.emptyList(), 0, 1000, 0).get(),select(VINDEX, 0, Collections.emptyList(), 0, 1000, 0).get());
                } catch (Exception e) {
                    throw new IllegalStateException("Can't resolve schema", e);
                }
            }

            @Override
            protected void setFutureResult(long code, AsyncQuery q) {
                if (q != null) {
                    if (code != 0) {
                        if (code == ER_SCHEMA_CHANGED) {
                            updateSchema();
                            if (!addQuery(q)) {
                                q.setError(new CommunicationException("Key is cancelled", error));
                            }
                        } else {
                            Object error = state.getBody().get(Key.ERROR);
                            q.setError(new TarantoolException((int) code, error instanceof String ? (String) error : new String((byte[]) error)));
                        }
                    } else {
                        q.setValue(resolveTuples(q.getCode(), q.getArgs(), (List) state.getBody().get(Key.DATA)));
                    }
                    if (q == spaces || q == indexes) {
                        if (spaces.isDone() && indexes.isDone()) {
                            try {
                                buildSchema(spaces.get(), indexes.get());
                                schemaId = (Long)state.getHeader().get(Key.SCHEMA_ID);
                            } catch (Exception e) {
                                delegate.close(e);
                            }
                            schemaUpdate.release();
                        }
                    }
                }
            }
        };
    }

    private void updateSchema() {
        if (schemaUpdate.tryAcquire()) {
            spaces = delegate.select(VSPACE, 0, Collections.emptyList(), 0, 1000, 0);
            indexes = delegate.select(VINDEX, 0, Collections.emptyList(), 0, 1000, 0);
        }
    }

    @Override
    protected long getSchemaId() {
        return delegate.getSchemaId();
    }

    @Override
    protected Object[] resolveArgs(Code code, Object[] args) {
        Object[] result;
        Long schemaId;
        do {
            schemaId = getSchemaId();
            result = super.resolveArgs(code, args, schemaId);
        } while (schemaId != null && !schemaId.equals(getSchemaId()));
        return result;
    }

    @Override
    public Future<List> exec(Code code, Object... args) {
        return delegate.exec(code, resolveArgs(code, args));
    }


    @Override
    public void auth(String username, String password) {
        throw new IllegalStateException("Should not be called for async connections");
    }

    @Override
    public void close() {
        delegate.close();
    }

    @Override
    public boolean isValid() {
        return delegate.isValid();
    }
}
