package org.tarantool.msgpack;


import org.msgpack.template.EnumWithId;

public enum Key implements EnumWithId<Key> {
    CODE(0), SYNC(0x01),
    SPACE(0x10), INDEX(0x11),
    LIMIT(0x12), OFFSET(0x13),
    ITERATOR(0x14), KEY(0x20),
    TUPLE(0x21), FUNCTION(0x22),
    USER_NAME(0x23),
    DATA(0x30), ERROR(0x31);

    int id;

    Key(int id) {
        this.id = id;
    }

    public int getId() {
        return id;
    }

    @Override
    public Key getById(int id) {
        for (Key key : Key.values()) {
            if (key.id == id) {
                return key;
            }
        }
        throw new IllegalArgumentException("Unknown Key with id: " + id);
    }


}
