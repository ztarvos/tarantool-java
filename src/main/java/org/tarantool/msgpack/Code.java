package org.tarantool.msgpack;


import org.msgpack.template.EnumWithId;

public enum Code implements EnumWithId<Code> {
    SELECT(1), INSERT(2), REPLACE(3), UPDATE(4),
    DELETE(5), CALL(6), AUTH(7), PING(64), SUBSCRIBE(66);

    int id;

    Code(int id) {
        this.id = id;
    }

    public int getId() {
        return id;
    }

    @Override
    public Code getById(int id) {
        for (Code code : Code.values()) {
            if (code.id == id) {
                return code;
            }
        }
        throw new IllegalArgumentException("Unknown code with id: " + id);
    }


}
