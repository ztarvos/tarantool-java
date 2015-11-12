package org.tarantool;

import java.io.IOException;
import java.util.Arrays;

public class TestClient16WithJackson {
    /*
     Before executing this test you should configure your local tarantool

     box.cfg{listen=3301}
     space = box.schema.space.create('tester2')
     box.space.tester2:create_index('primary', {type = 'hash', parts = {1, 'NUM'}})

     box.schema.user.create('test', { password = 'test' })
     box.schema.user.grant('test', 'execute,read,write', 'universe')
     box.space['tester2']:format{{name='id', type='num'},{name='age', type='num'},{name='name', type='str'},{name='male', type='str'},{name='tags', type='array'},{name='links',type='array'}}


    */


    public static void main(String[] args) throws IOException {
        TarantoolGenericConnection16 con = new TarantoolGenericConnection16Impl("localhost", 3301, new JacksonMapper());
        con.auth("test", "test");
        final TestSchema2 schema = con.schema(new TestSchema2());
        System.out.println(schema);
        Pojo[] delete = con.delete(Pojo[].class, schema.tester2.id, Arrays.asList(1));
        System.out.println(Arrays.toString(delete));

        Pojo[] insert = con.insert(Pojo[].class, schema.tester2.id, new Pojo());
        System.out.println(Arrays.toString(insert));

        Pojo[] select0 = con.select(Pojo[].class, schema.tester2.id, schema.tester2.primary, Arrays.asList(1), 0, 100, 0);
        System.out.println(Arrays.toString(select0));

        Pojo[] update0 = con.update(Pojo[].class, schema.tester2.id, Arrays.asList(1), Arrays.asList("=", schema.tester2.fields.age, 66));
        System.out.println(Arrays.toString(update0));

        Pojo[] eval = con.eval(Pojo[].class, "return {age=99}"); //age should be overriden
        System.out.println(Arrays.toString(eval));


        Pojo eval2 = con.eval(Pojo.class,"return 1,99,'hello',false,{'no'},{4,5,6}");
        System.out.println(eval2);

        Pojo[] eval3= con.eval(Pojo[].class,"return {1,99,'hello',false,{'no'},{4,5,6}}");
        System.out.println(Arrays.toString(eval3));

        con.upsert(Pojo[].class,schema.tester2.id, Arrays.asList(1), new Pojo(), Arrays.asList("=", schema.tester2.fields.age, 999));

        Pojo[] delete2 = con.delete(Pojo[].class, schema.tester2.id, Arrays.asList(2));

        System.out.println(Arrays.toString(delete2));

        final Pojo def = new Pojo();
        def.id = 2;
        con.upsert(Pojo[].class,schema.tester2.id, Arrays.asList(2), def,Arrays.asList("=", schema.tester2.fields.age, 998));



        Pojo[] select1 = con.select(Pojo[].class, schema.tester2.id, schema.tester2.primary, Arrays.asList(1), 0, 100, 0);
        System.out.println(Arrays.toString(select1));

        Pojo[] select2 = con.select(Pojo[].class, schema.tester2.id, schema.tester2.primary, Arrays.asList(2), 0, 100, 0);
        System.out.println(Arrays.toString(select2));

        con.close();
    }
}
