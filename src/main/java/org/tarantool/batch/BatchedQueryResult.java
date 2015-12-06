package org.tarantool.batch;

import java.util.List;

import org.tarantool.TarantoolException;

public class BatchedQueryResult {
    private List result;
    private TarantoolException error;

    public BatchedQueryResult(List result) {
        this.result = result;
    }

    public BatchedQueryResult(TarantoolException error) {
        this.error = error;
    }

    public BatchedQueryResult() {
    }

    public List getResult() {
        return result;
    }

    public void setResult(List result) {
        this.result = result;
    }

    public TarantoolException getError() {
        return error;
    }

    public void setError(TarantoolException error) {
        this.error = error;
    }

    @Override
    public String toString() {
        if (result != null) {
            return result.toString();
        }
        return error.toString();
    }
}
