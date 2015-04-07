package org.tarantool;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.channels.SocketChannel;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

public class TestBatch16 {
    /*
     Before executing this test you should configure your local tarantool

     box.cfg{listen=3301}
     box.schema.space.create('tester')
     box.space.tester:create_index('primary', {type = 'hash', parts = {1, 'NUM'}})

     box.schema.user.create('test', { password = 'test' })
     box.schema.user.grant('test', 'execute,read,write', 'universe')


    */
    public static void main(String[] args) throws IOException {
        BatchConnection16 con = new BatchConnection16Impl(SocketChannel.open(new InetSocketAddress("localhost", 3301)));
        con.auth("test", "test");

        int spaceId = (Integer)con.eval("return box.space.tester.id").get(0);

        con.begin();

        List delete0 = con.delete(spaceId, Arrays.asList(0));

        List delete = con.delete(spaceId, Arrays.asList(1));

        List insert = con.insert(spaceId, Arrays.asList(1, "hello"));

        List insert2 = con.replace(spaceId, Arrays.asList(2, Collections.singletonMap("hello", "word"),new String[]{"a","b","c"}));

        List select0 = con.select(spaceId, 0, Arrays.asList(1), 0, 100, 0);
        ;
        List update0 = con.update(spaceId, Arrays.asList(1), Arrays.asList("=", 1, "Тариф индивидуальный 1%."));

        List result = con.call("math.ceil", 1.3);

        List eval = con.eval("return ...", 1, 2, 3);

        con.end();
        con.get();
        System.out.println(delete0);
        System.out.println(delete);
        System.out.println(insert);
        System.out.println(insert2);
        System.out.println(select0);
        System.out.println(update0);
        System.out.println(result);
        System.out.println(eval);
        ;
        con.close();

    }
}
