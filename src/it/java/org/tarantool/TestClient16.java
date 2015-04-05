package org.tarantool;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;


public class TestClient16 {
    /*
      Before executing this test you should configure your local tarantool

      box.cfg{listen=3301}
      box.schema.space.create('tester')
      box.space.tester:create_index('primary', {type = 'hash', parts = {1, 'NUM'}})

      box.schema.user.create('test', { password = 'test' })
      box.schema.user.grant('test', 'execute,read,write', 'universe')


     */
    public static void main(String[] args) throws IOException {
        TarantoolConnection16 con = new TarantoolConnection16Impl("localhost", 3301);
        con.auth("test", "test");

        int spaceId = (Integer) con.eval("return box.space.tester.id").get(0);

        List delete0 = con.delete(spaceId, Arrays.asList(0));
        System.out.println(delete0);
        List delete = con.delete(spaceId, Arrays.asList(1));
        System.out.println(delete);
        List insert = con.insert(spaceId, Arrays.asList(1, "hello"));
        System.out.println(insert);
        List insert2 = con.replace(spaceId, Arrays.asList(2, Collections.singletonMap("hello", "word"),new String[]{"a","b","c"}));
        System.out.println(insert2);
        List select0 = con.select(spaceId, 0, Arrays.asList(1), 0, 100, 0);
        System.out.println(select0);
        List update0 = con.update(spaceId, Arrays.asList(1), Arrays.asList("=", 1, "Hello"));
        System.out.println(update0);
        List result = con.call("math.ceil", 1.3);
        System.out.println(result);
        List eval = con.eval("return ...", 1, 2, 3);
        System.out.println(eval);
        con.close();

    }
}
