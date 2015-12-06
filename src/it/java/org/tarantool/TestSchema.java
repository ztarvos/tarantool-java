package org.tarantool;


import org.tarantool.schema.FieldsMapping;
import org.tarantool.schema.IndexId;
import org.tarantool.schema.SchemaId;
import org.tarantool.schema.Space;
import org.tarantool.schema.SpaceId;

public class TestSchema {

    @SchemaId
    long schemaId;


    @Space
    public SpaceWithPK tester = new SpaceWithPK();

    public class SpaceFields {
        public int id;
        public int text;

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
                "schemaId=" + schemaId +
                ", tester=" + tester +
                '}';
    }
}


