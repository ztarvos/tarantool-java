package org.tarantool;


import org.tarantool.schema.FieldsMapping;
import org.tarantool.schema.IndexId;
import org.tarantool.schema.Space;
import org.tarantool.schema.SpaceId;

public class TestSchema2 {

    @Space
    public SpaceWithPK tester2 = new SpaceWithPK();

    public class SpaceFields {
        public int id;
        public int age;
        public int name;
        public int male;
        public int tags;
        public int links;
    }

    public class SpaceWithPK {
        @SpaceId
        public int id;

        @IndexId
        public int primary;

        @FieldsMapping
        public SpaceFields fields = new SpaceFields();

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
                "tester=" + tester2 +
                '}';
    }
}


