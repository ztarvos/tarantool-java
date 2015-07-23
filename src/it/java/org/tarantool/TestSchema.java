package org.tarantool;


import org.tarantool.schema.IndexId;
import org.tarantool.schema.Space;
import org.tarantool.schema.SpaceId;

public class TestSchema {

    @Space
    public SpaceWithPK tester = new SpaceWithPK();

    public class SpaceWithPK {
        @SpaceId
        public int id;

        @IndexId
        public int primary;

        @Override
        public String toString() {
            return "SpaceWithPK{" +
                    "id=" + id +
                    ", primary=" + primary +
                    '}';
        }
    }

    @Override
    public String toString() {
        return "TestSchema{" +
                "tester=" + tester +
                '}';
    }
}


