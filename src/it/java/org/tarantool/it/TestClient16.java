package org.tarantool.it;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;

import org.junit.Test;
import org.msgpack.MessagePack;
import org.msgpack.type.Value;
import org.tarantool.msgpack.TarantoolConnection16;
import org.tarantool.pool.SocketChannelPooledConnectionFactory16;

public class TestClient16 {
    @org.msgpack.annotation.Message
    public static class Message {
        int id = 0;
        String name = "a";
    }

    @Test
    void testClient16() throws IOException {
        SocketChannelPooledConnectionFactory16 factory = new SocketChannelPooledConnectionFactory16();

        TarantoolConnection16 con = factory.getConnection();
        con.auth("test", "test");

        Message[] str = new Message[]{new Message()};
        List<Value> delete0 = con.delete(0, Arrays.asList(0));
        List<Value> delete = con.delete(0, Arrays.asList(1));
        List<Value> insert = con.insert(0, Arrays.asList(1, "hello"));
        Message[] insert1 = con.insert(Message[].class, 0, new Message());
        List<Value> select0 = con.select(0, 0, Arrays.asList(1), 0, 100);
        Message[] select = con.select(Message[].class, 0, 0, new int[]{0}, 0, 10);
        List<Value> time = con.call("box.time");
        float[][] time2 = con.call(float[][].class, "box.time");
        Message[] updated = con.update(Message[].class, 0, new int[]{0}, Arrays.asList(Arrays.asList("=", 1, "!!!")));
        factory.returnConnection(con);
        factory.close();


    }
}
