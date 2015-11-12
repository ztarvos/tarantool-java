package org.tarantool;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;


public class TestClient16 {
    /*
      Before executing this test you should configure your local tarantool

      box.cfg{listen=3301}
      space = box.schema.space.create('tester')
      box.space.tester:create_index('primary', {type = 'hash', parts = {1, 'NUM'}})

      box.schema.user.create('test', { password = 'test' })
      box.schema.user.grant('test', 'execute,read,write', 'universe')
      box.space.tester:format{{name='id',type='num'},{name='text',type='str'}}
     */
    public static void main(String[] args) throws IOException {
        TarantoolConnection16 con = new TarantoolConnection16Impl("localhost", 3301);
        con.auth("test", "test");

        final TestSchema schema = con.schema(new TestSchema());
        System.out.println(schema);

        List delete0 = con.delete(schema.tester.id, Arrays.asList(0));
        System.out.println(delete0);
        List delete = con.delete(schema.tester.id, Arrays.asList(1));
        System.out.println(delete);
        List insert = con.insert(schema.tester.id, Arrays.asList(1, "hello"));
        System.out.println(insert);
        List insert2 = con.replace(schema.tester.id, Arrays.asList(2, Collections.singletonMap("hello", "word"),new String[]{"a","b","c"}));
        System.out.println(insert2);
        List select0 = con.select(schema.tester.id, schema.tester.primary, Arrays.asList(1), 0, 100, 0);
        System.out.println(select0);
        List update0 = con.update(schema.tester.id, Arrays.asList(1), Arrays.asList("=", 1, "Hello"));
        System.out.println(update0);

        con.upsert(schema.tester.id, Arrays.asList(1), Arrays.asList(1, "hello"), Arrays.asList("=", schema.tester.fields.text, "Hello World!!!"));
        con.upsert(schema.tester.id, Arrays.asList(2), Arrays.asList(2, "hello"),Arrays.asList("=", schema.tester.fields.text, "Hello World!!!"));
        List select1 = con.select(schema.tester.id, schema.tester.primary, Arrays.asList(1), 0, 100, 0);
        System.out.println(select1);
        List select2 = con.select(schema.tester.id, schema.tester.primary, Arrays.asList(2), 0, 100, 0);
        System.out.println(select2);
        List result = con.call("math.ceil", 1.3);
        System.out.println(result);
        List eval = con.eval("return ...", 1, 2, 3);
        System.out.println(eval);
        con.close();

    }
}
