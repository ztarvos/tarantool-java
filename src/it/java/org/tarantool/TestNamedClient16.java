package org.tarantool;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.channels.SocketChannel;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.tarantool.named.TarantoolNamedConnection16;
import org.tarantool.named.TarantoolNamedConnection16Impl;
import org.tarantool.named.UpdateOperation;


public class TestNamedClient16 {
    /*
      Before executing this test you should configure your local tarantool

      box.cfg{listen=3301}
      space = box.schema.space.create('tester')
      box.space.tester:create_index('primary', {type = 'hash', parts = {1, 'NUM'}})

      box.schema.user.create('test', { password = 'test' })
      box.schema.user.grant('test', 'execute,read,write,create,drop','universe')
      box.space.tester:format{{name='id',type='num'},{name='text',type='str'}}
     */
    public static Map<String,Object> map(Object ...args) {
        Map<String,Object> map = new HashMap<String,Object>();
        for(int i=0;i<args.length;i+=2) {
            map.put((String)args[i],args[i+1]);
        }
        return map;
    }
    public static void main(String[] args) throws IOException, InterruptedException {
        TarantoolNamedConnection16Impl con = new TarantoolNamedConnection16Impl(SocketChannel.open(new InetSocketAddress("localhost", 3301)));
        con.auth("test", "test");

        con.setSchemaId(1L);

        List delete0 = con.delete("tester", map("id",0));
        System.out.println(delete0);
        List delete = con.delete("tester", map("id", 1));
        System.out.println(delete);
        List insert = con.insert("tester", map("id", 1, "text", "hello"));

        System.out.println(insert);
        List insert2 = con.replace("tester", map("id",2, "text",Collections.singletonMap("hello", "word")));
        System.out.println(insert2);
        List select0 = con.select("tester", "primary", map("id", 1), 0, 100, 0);
        System.out.println(select0);
        List update0 = con.update("tester", map("id",1), new UpdateOperation("=", "text", "Hello"));
        System.out.println(update0);


        con.upsert("tester", map("id", 1), map("id", 1, "text", "hello"), new UpdateOperation("=", "text", "Hello World!!!"));
        con.upsert("tester",map("id", 2), map("id", 2, "text", "hello"), new UpdateOperation("=", "text", "Hello World!!!"));
        List select1 = con.select("tester", "primary", map("id", 1), 0, 100, 0);
        System.out.println(select1);
        List select2 = con.select("tester", "primary",map("id", 2), 0, 100, 0);
        System.out.println(select2);
        List result = con.call("math.ceil", 1.3);
        System.out.println(result);
        List eval = con.eval("return ...", 1, 2, 3);
        System.out.println(eval);
        con.close();

    }
}
