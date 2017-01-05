package org.tarantool;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.nio.channels.SocketChannel;
import java.util.Collections;

public class TestTarantoolConnection {
    public static void main(String[] args) throws IOException {
        Socket socket = new Socket();
        socket.connect(new InetSocketAddress("127.0.0.1", 3301));
        TarantoolConnection con = new TarantoolConnection("test", "test", socket);
        System.out.println(con.select(281,0, Collections.emptyList(),0,1024,0));
        System.out.println(con.call("box.begin"));
        con.close();
    }
}
