package org.tarantool.batch;

import java.nio.channels.SocketChannel;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

import org.tarantool.Code;
import org.tarantool.Key;
import org.tarantool.TarantoolConnection16Base;
import org.tarantool.TarantoolException;

public class TarantoolBatchConnection16Impl extends TarantoolConnection16Base<Integer,Object,Object,BatchedQueryResult> implements TarantoolBatchConnection16 {
    protected final AtomicLong syncId = new AtomicLong(0);
    protected Map<Long, BatchedQuery> batch;
    protected boolean sent = false;

    @Override
    public void begin() {
        batch = new LinkedHashMap<Long, BatchedQuery>();
    }

    @Override
    public void end() {
        for (Map.Entry<Long, BatchedQuery> entry : batch.entrySet()) {
            BatchedQuery q = entry.getValue();
            write(state.pack(q.code, entry.getKey(), q.args));
        }
        sent = true;
    }

    @Override
    public void get() {
        if (!sent) {
            throw new IllegalStateException("Batch is not sent you should end it first");
        }
        while (!batch.isEmpty()) {
            try {
                readPacket();
                Long syncId = (Long) state.getHeader().get(Key.SYNC);
                BatchedQuery q = batch.remove(syncId);
                q.result.setResult((List) state.getBody().get(Key.DATA));
            } catch (TarantoolException e) {
                Long syncId = (Long) state.getHeader().get(Key.SYNC);
                BatchedQuery q = batch.remove(syncId);
                q.result.setError(e);
            }
        }
        batch = null;
        sent = false;
    }

    public TarantoolBatchConnection16Impl(SocketChannel channel) {
        super(channel);
    }

    @Override
    public BatchedQueryResult exec(Code code, Object... args) {
        if (batch == null) {
            write(state.pack(code, args));
            try {
                return new BatchedQueryResult((List) readData());
            } catch (TarantoolException e) {
                return new BatchedQueryResult(e);
            }
        }
        if (sent) {
            throw new IllegalStateException("Batch is already sent. Call get to read it");
        }
        BatchedQueryResult result = new BatchedQueryResult();
        BatchedQuery q = new BatchedQuery(code, args, result);
        addQuery(q);
        return result;
    }

    public void addQuery(BatchedQuery q) {
        this.batch.put(syncId.incrementAndGet(), q);
    }

    public Map<Long, BatchedQuery> getBatch() {
        return batch;
    }

    public void setBatch(Map<Long, BatchedQuery> batch) {
        this.batch = batch;
    }
}
