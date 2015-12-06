package org.tarantool.batch;

import org.tarantool.TarantoolConnection16Ops;

public interface TarantoolBatchConnection16 extends TarantoolConnection16Ops<Integer,Object,Object,BatchedQueryResult> {

    void begin();

    void end();

    void get();

}
