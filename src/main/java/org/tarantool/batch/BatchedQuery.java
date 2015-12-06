package org.tarantool.batch;

import org.tarantool.Code;

public class BatchedQuery {
    public final Code code;
    public final Object[] args;
    public final BatchedQueryResult result;

    public BatchedQuery(Code code, Object[] args, BatchedQueryResult result) {
        this.code = code;
        this.args = args;
        this.result = result;
    }
}
