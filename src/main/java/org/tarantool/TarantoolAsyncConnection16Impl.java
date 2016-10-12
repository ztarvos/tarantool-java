package org.tarantool;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.ClosedChannelException;
import java.nio.channels.SelectionKey;
import java.nio.channels.SocketChannel;
import java.util.List;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

public class TarantoolAsyncConnection16Impl implements TarantoolSelectorWorker.ChannelProcessor, TarantoolAsyncConnection16 {
    protected static final int ST_LENGTH = 0;
    protected static final int ST_BODY = 1;
    protected volatile SelectionKey key;
    protected volatile SocketChannel channel;
    protected AtomicLong syncId = new AtomicLong(0);
    protected final ConnectionState readState = new ConnectionState();
    protected ByteBuffer readBuffer;
    protected final ConnectionState writeState = new ConnectionState();
    protected ByteBuffer writeBuffer;
    protected LinkedBlockingQueue<AsyncQuery> writeQueue = new LinkedBlockingQueue<AsyncQuery>();
    protected Map<Long, AsyncQuery> futures = new ConcurrentHashMap<Long, AsyncQuery>();

    protected int state = ST_LENGTH;
    protected volatile Exception error;


    public TarantoolAsyncConnection16Impl(TarantoolSelectorWorker worker, SocketChannel channel, String username, String password, long timeout, TimeUnit unit) {
        TarantoolConnection16Impl connection = new TarantoolConnection16Impl(channel);
        if (username != null) {
            connection.auth(username, password);
        }
        BlockingQueue<SelectionKey> queue = worker.register(connection.getChannel(), this);
        try {
            key = queue == null ? null : queue.poll(timeout, unit);
        } catch (InterruptedException e) {
            throw new CommunicationException("Can't register key", e);
        }
        if (key == null) {
            connection.close();
            throw new CommunicationException("Can't register key");
        }
        this.channel = channel;
        readBuffer = readState.getLengthReadBuffer();
    }

    @Override
    public void idle() {
        key.interestOps(SelectionKey.OP_READ | (writeQueue.isEmpty() ? 0 : SelectionKey.OP_WRITE));
    }

    @Override
    public void read() {
        try {
            int read = channel.read(readBuffer);
            if (read < 0) {
                close(new ClosedChannelException());
            }
            if (read > 0) {
                if (readBuffer.position() == readBuffer.limit()) {
                    if (state == ST_LENGTH) {
                        readBuffer = readState.getPacketReadBuffer();
                        state = ST_BODY;
                        read();
                    } else if (state == ST_BODY) {
                        readState.unpack();
                        long code = (Long) readState.getHeader().get(Key.CODE);
                        long syncId = (Long) readState.getHeader().get(Key.SYNC);
                        AsyncQuery q = futures.remove(syncId);
                        if (q != null) {
                            if (code != 0) {
                                Object error = readState.getBody().get(Key.ERROR);
                                q.setError(new TarantoolException((int) code, error instanceof String ? (String) error : new String((byte[]) error)));
                            } else {
                                q.setValue(readState.getBody().get(Key.DATA));
                            }

                        }
                        readBuffer = readState.getLengthReadBuffer();
                        state = ST_LENGTH;
                    }
                }
            }
        } catch (IOException e) {
            close(e);
        }
    }

    @Override
    public void write() {
        if (writeBuffer == null) {
            AsyncQuery query = writeQueue.poll();
            try {
                if (query != null) {
                    writeBuffer = writeState.pack(query.code, query.id, query.args);
                }
            } catch (Exception e) {
                query.setError(e);
            }
        }
        if (writeBuffer != null) {
            try {
                channel.write(writeBuffer);
                if (writeBuffer.remaining() == 0) {
                    writeBuffer = null;
                }
            } catch (IOException e) {
                close(e);
            }
        }
    }


    protected <T> Future<T> exec(Code code, Object... args) {
        if (key.isValid()) {
            AsyncQuery q = new AsyncQuery(syncId.incrementAndGet(), code, args);
            futures.put(q.id, q);
            writeQueue.add(q);
            if (key.isValid()) {
                key.selector().wakeup();
                return q;
            } else {
                q.setError(error);
            }
        }
        throw new CommunicationException("Key is cancelled", error);
    }

    @Override
    public Future<List> select(int space, int index, Object key, int offset, int limit, int iterator) {
        return exec(Code.SELECT, Key.SPACE, space, Key.INDEX, index, Key.KEY, key, Key.ITERATOR, iterator, Key.LIMIT, limit);
    }

    @Override
    public Future<List> insert(int space, Object tuple) {
        return exec(Code.INSERT, Key.SPACE, space, Key.TUPLE, tuple);
    }

    @Override
    public Future<List> replace(int space, Object tuple) {
        return exec(Code.REPLACE, Key.SPACE, space, Key.TUPLE, tuple);
    }

    @Override
    public Future<List> update(int space, Object key, Object... args) {
        return exec(Code.UPDATE, Key.SPACE, space, Key.KEY, key, Key.TUPLE, args);
    }

    @Override
    public Future<List> delete(int space, Object key) {
        return exec(Code.DELETE, Key.SPACE, space, Key.KEY, key);
    }

    @Override
    public Future<List> call(String function, Object... args) {
        return exec(Code.CALL, Key.FUNCTION, function, Key.TUPLE, args);
    }

    @Override
    public Future<List> call17(String function, Object... args) {
        return exec(Code.CALL17, Key.FUNCTION, function, Key.TUPLE, args);
    }

    @Override
    public Future<List> eval(String expression, Object... args) {
        return exec(Code.EVAL, Key.EXPRESSION, expression, Key.TUPLE, args);
    }

    @Override
    public void upsert(int space, Object key, Object def, Object... args) {
        exec(Code.UPSERT, Key.SPACE, space, Key.KEY, key, Key.TUPLE, def, Key.UPSERT_OPS, args);
    }

    @Override
    public void close() {
        close(null);
    }

    @Override
    public boolean isValid() {
        return key.isValid();
    }

    public void close(Exception e) {
        error = e;
        try {
            if (key != null) {
                key.cancel();
            }
            channel.close();
        } catch (Exception ignored) {
        }
        for (AsyncQuery q : futures.values()) {
            q.setError(error);
        }
    }

}
