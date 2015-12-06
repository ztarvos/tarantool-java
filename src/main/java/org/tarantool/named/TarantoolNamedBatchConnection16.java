package org.tarantool.named;

import java.util.Map;

import org.tarantool.TarantoolConnection16Ops;
import org.tarantool.batch.BatchedQueryResult;

public interface TarantoolNamedBatchConnection16 extends TarantoolConnection16Ops<String,Map<String,Object>,UpdateOperation,BatchedQueryResult> {
    void begin();

    void end();

    void get();
}
