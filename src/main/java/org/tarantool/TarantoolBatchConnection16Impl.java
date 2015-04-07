package org.tarantool;

import java.nio.channels.SocketChannel;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

public class TarantoolBatchConnection16Impl extends TarantoolConnection16Base implements TarantoolBatchConnection16 {
    protected final AtomicLong syncId = new AtomicLong(0);
    protected Map<Long, Q> batch;
    protected boolean sent = false;

    @Override
    public void begin() {
        batch = new LinkedHashMap<Long, Q>();
    }

    @Override
    public void end() {
        for (Map.Entry<Long, Q> entry : batch.entrySet()) {
            Q q = entry.getValue();
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
            readPacket();
            Long syncId = (Long) state.getHeader().get(Key.SYNC);
            Q q = batch.remove(syncId);
            q.result.addAll((List) state.getBody().get(Key.DATA));
        }
        batch = null;
        sent = false;
    }

    private static class Q {
        Code code;
        Object[] args;
        List result;

        public Q(Code code, Object[] args, List result) {
            this.code = code;
            this.args = args;
            this.result = result;
        }
    }

    public TarantoolBatchConnection16Impl(SocketChannel channel) {
        super(channel);
    }

    @Override
    protected List exec(Code code, Object... args) {
        if (batch == null) {
            write(state.pack(code, args));
            return (List) read();
        }
        if (sent) {
            throw new IllegalStateException("Batch is already sent. Call get to read it");
        }
        ArrayList result = new ArrayList();
        this.batch.put(syncId.incrementAndGet(), new Q(code, args, result));
        return result;
    }


}
