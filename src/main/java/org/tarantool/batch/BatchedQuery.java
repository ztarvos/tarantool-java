package org.tarantool.batch;

import org.tarantool.Code;

public class BatchedQuery {
    public final Code code;
    public final long id;
    public final Object[] args;
    public final BatchedQueryResult result;

    public BatchedQuery(long id,Code code, Object[] args, BatchedQueryResult result) {
        this.id = id;
        this.code = code;
        this.args = args;
        this.result = result;
    }
}
