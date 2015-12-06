package org.tarantool.named;

public class UpdateOperation {
    private final String op;
    private final  String field;
    private  final Object[] arguments;

    public UpdateOperation(String code, String field, Object... arguments) {
        this.op = code;
        this.field = field;
        this.arguments = arguments;
    }

    public String getOp() {
        return op;
    }
    public String getField() {
        return field;
    }

    public Object[] getArguments() {
        return arguments;
    }
}
