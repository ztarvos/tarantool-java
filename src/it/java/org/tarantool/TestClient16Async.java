package org.tarantool;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.URISyntaxException;
import java.nio.channels.SelectionKey;
import java.nio.channels.SocketChannel;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

public class TestClient16Async {
    /*
    Before executing this test you should configure your local tarantool

    box.cfg{listen=3301}
    box.schema.space.create('tester2')
    box.space.tester2:create_index('primary', {type = 'hash', parts = {1, 'NUM'}})

    box.schema.user.create('test', { password = 'test' })
    box.schema.user.grant('test', 'execute,read,write', 'universe')


   */
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
        final TarantoolAsyncConnection16 con = new TarantoolAsyncConnection16Impl(worker, SocketChannel.open(new InetSocketAddress("localhost", 3301)), "test", "test", 100, TimeUnit.MILLISECONDS);
        for (int i = 0; i < 16; i++) {
            exec.execute(new Runnable() {
                @Override
                public void run() {
                    for (int i = 0; i < 10; i++) {
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
