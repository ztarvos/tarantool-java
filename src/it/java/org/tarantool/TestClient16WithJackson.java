package org.tarantool;

import java.io.IOException;
import java.util.Arrays;

public class TestClient16WithJackson {
    /*
     Before executing this test you should configure your local tarantool

     box.cfg{listen=3301}
     box.schema.space.create('tester2')
     box.space.tester2:create_index('primary', {type = 'hash', parts = {1, 'NUM'}})

     box.schema.user.create('test', { password = 'test' })
     box.schema.user.grant('test', 'execute,read,write', 'universe')


    */




    public static void main(String[] args) throws IOException {
        TarantoolGenericConnection16 con = new TarantoolGenericConnection16Impl("localhost", 3301, new JacksonMapper());
        con.auth("test", "test");
        final TestSchema schema = con.schema(new TestSchema());
        System.out.println(schema);
        Pojo[] delete = con.delete(Pojo[].class, schema.tester.id, Arrays.asList(1));
        System.out.println(Arrays.toString(delete));

        Pojo[] insert = con.insert(Pojo[].class, schema.tester.id, new Pojo());
        System.out.println(Arrays.toString(insert));

        Pojo[] select0 = con.select(Pojo[].class, schema.tester.id, schema.tester.primary, Arrays.asList(1), 0, 100, 0);
        System.out.println(Arrays.toString(select0));

        Pojo[] update0 = con.update(Pojo[].class, schema.tester.id, Arrays.asList(1), Arrays.asList("=", 1, 66));
        System.out.println(Arrays.toString(update0));

        Pojo[] eval = con.eval(Pojo[].class, "return {age=99}"); //age should be overriden
        System.out.println(Arrays.toString(eval));


        Pojo eval2 = con.eval(Pojo.class,"return 1,99,'hello',false,{'no'},{4,5,6}");
        System.out.println(eval2);

        Pojo[] eval3= con.eval(Pojo[].class,"return {1,99,'hello',false,{'no'},{4,5,6}}");
        System.out.println(Arrays.toString(eval3));
        con.close();
    }
}
