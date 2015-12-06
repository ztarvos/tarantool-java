package org.tarantool;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.URISyntaxException;
import java.nio.channels.SelectionKey;
import java.nio.channels.SocketChannel;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import org.tarantool.async.TarantoolAsyncConnection16;
import org.tarantool.async.TarantoolAsyncConnection16Impl;
import org.tarantool.async.TarantoolSelectorWorker;
import org.tarantool.named.TarantoolAsyncNamedConnection16;
import org.tarantool.named.TarantoolAsyncNamedConnection16Impl;
import org.tarantool.named.UpdateOperation;

public class TestClient16NamedAsync {
    /*
    Before executing this test you should configure your local tarantool

    box.cfg{listen=3301}
    box.schema.space.create('tester')
    box.space.tester2:create_index('primary', {type = 'hash', parts = {1, 'NUM'}})

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
    public static void main(String[] args)
            throws IOException, ExecutionException, InterruptedException, URISyntaxException {
        TarantoolSelectorWorker worker = new TarantoolSelectorWorker() {

            @Override
            public void error(SelectionKey key, Exception e) {
                e.printStackTrace();
            }
        };
        ExecutorService exec = Executors.newFixedThreadPool(16);
        Thread thread = new Thread(worker);
        thread.start();
        final TarantoolAsyncNamedConnection16Impl con = new TarantoolAsyncNamedConnection16Impl(worker, SocketChannel.open(new InetSocketAddress("localhost", 3301)), "test", "test", 100, TimeUnit.MILLISECONDS);
        con.setSchemaId(1L);
        Future<List> delete0 = con.delete("tester", map("id",0));
        System.out.println(delete0.get());
        Future<List> delete = con.delete("tester", map("id", 1));
        System.out.println(delete.get());
        Future<List> insert = con.insert("tester", map("id", 1, "text", "hello"));
        System.out.println(insert.get());
        Future<List> insert2 = con.replace("tester", map("id",2, "text", Collections.singletonMap("hello", "word")));
        System.out.println(insert2.get());
        Future<List> select0 = con.select("tester", "primary", map("id", 1), 0, 100, 0);
        System.out.println(select0.get());
        Future<List> update0 = con.update("tester", map("id",1), new UpdateOperation("=", "text", "Hello"));
        System.out.println(update0.get());

        con.upsert("tester", map("id", 1), map("id", 1, "text", "hello"), new UpdateOperation("=", "text", "Hello World!!!"));
        con.upsert("tester",map("id", 2), map("id", 2, "text", "hello"), new UpdateOperation("=", "text", "Hello World!!!"));
        Future<List> select1 = con.select("tester", "primary", map("id", 1), 0, 100, 0);
        System.out.println(select1.get());
        Future<List> select2 = con.select("tester", "primary",map("id", 2), 0, 100, 0);
        System.out.println(select2.get());
        Future<List> result = con.call("math.ceil", 1.3);
        System.out.println(result.get());
        Future<List> eval = con.eval("return ...", 1, 2, 3);
        System.out.println(eval.get());

        for (int i = 0; i < 16; i++) {
            exec.execute(new Runnable() {
                @Override
                public void run() {
                    for (int i = 0; i < 10; i++) {
                        if (con.isValid()) {
                            Future<List> call = con.call("math.ceil", 1.3);
                            Future<List> eval = con.eval("return ...", 1, 2, 3);
                            System.out.println(call.isDone() + " " + eval.isDone());
                            try {
                                List cr = call.get();
                                List er = eval.get();
                                System.out.println(cr);
                                System.out.println(er);
                            } catch (Exception e) {
                                e.printStackTrace();
                            }
                        } else {
                            System.out.println("Connection is not valid");
                        }

                    }
                }
            });

        }
        exec.shutdown();
        exec.awaitTermination(1, TimeUnit.SECONDS);
        con.close();
        thread.interrupt();
    }
}
