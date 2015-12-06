package org.tarantool;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.channels.SocketChannel;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.tarantool.batch.BatchedQueryResult;
import org.tarantool.named.TarantoolNamedBatchConnection16;
import org.tarantool.named.TarantoolNamedBatchConnection16Impl;
import org.tarantool.named.UpdateOperation;

public class TestNamedBatch16 {
    /*
     Before executing this test you should configure your local tarantool

     box.cfg{listen=3301}
     box.schema.space.create('tester')
     box.space.tester:create_index('primary', {type = 'hash', parts = {1, 'NUM'}})

     box.schema.user.create('test', { password = 'test' })
     box.schema.user.grant('test', 'execute,read,write', 'universe')
     box.space.tester:format{{name='id',type='num'},{name='text',type='str'}}


    */
    public static Map<String,Object> map(Object ...args) {
        Map<String,Object> map = new HashMap<String,Object>();
        for(int i=0;i<args.length;i+=2) {
            map.put((String)args[i],args[i+1]);
        }
        return map;
    }
    public static void main(String[] args) throws IOException {
        TarantoolNamedBatchConnection16Impl con = new TarantoolNamedBatchConnection16Impl(SocketChannel.open(new InetSocketAddress("localhost", 3301)));
        con.auth("test", "test");

        con.setSchemaId(1L);

        con.begin();

        BatchedQueryResult delete0 = con.delete("tester", map("id",0));

        BatchedQueryResult delete = con.delete("tester", map("id", 1));

        BatchedQueryResult insert = con.insert("tester", map("id", 1, "text", "hello"));

        BatchedQueryResult insert2 = con.replace("tester", map("id",2, "text",Collections.singletonMap("hello", "word")));

        BatchedQueryResult select0 = con.select("tester", "primary", map("id", 1), 0, 100, 0);

        con.setSchemaId(1L);

        BatchedQueryResult update0 = con.update("tester", map("id",1), new UpdateOperation("=", "text", "Hello"));

        con.upsert("tester", map("id", 1), map("id", 1, "text", "hello"), new UpdateOperation("=", "text", "Hello World!!!"));
        con.upsert("tester",map("id", 2), map("id", 2, "text", "hello"), new UpdateOperation("=", "text", "Hello World!!!"));
        BatchedQueryResult select1 = con.select("tester", "primary", map("id", 1), 0, 100, 0);
        BatchedQueryResult select2 = con.select("tester", "primary",map("id", 2), 0, 100, 0);

        BatchedQueryResult result = con.call("math.ceil", 1.3);

        BatchedQueryResult eval = con.eval("return ...", 1, 2, 3);


        con.end();
        con.get();
        System.out.println(delete0);
        System.out.println(delete);
        System.out.println(insert);
        System.out.println(insert2);
        System.out.println(select0);
        System.out.println(update0);
        System.out.println(select1);
        System.out.println(select2);
        System.out.println(result);
        System.out.println(eval);
        con.close();

    }
}
