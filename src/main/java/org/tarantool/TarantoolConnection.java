package org.tarantool;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.Socket;
import java.nio.ByteBuffer;
import java.util.List;

public class TarantoolConnection extends TarantoolBase<List<?>> {
    protected InputStream in;
    protected OutputStream out;
    protected Socket socket;


    public TarantoolConnection(String username, String password, Socket socket) throws IOException {
        super(username, password, socket);
        this.socket = socket;
        this.out = socket.getOutputStream();
        this.in = socket.getInputStream();
    }

    @Override
    public List<?> exec(Code code, Object... args) {
        try {
            ByteBuffer packet = createPacket(code, syncId.incrementAndGet(), null, args);
            out.write(packet.array(), 0, packet.remaining());
            out.flush();
            readPacket(is);
            Long c = (Long) headers.get(Key.CODE.getId());
            if (c == 0) {
                return (List) body.get(Key.DATA.getId());
            } else {
                throw serverError(c, body.get(Key.ERROR.getId()));
            }
        } catch (IOException e) {
            close();
            throw new CommunicationException("Couldn't execute query", e);
        }
    }

    public void begin() {
        call("box.begin");
    }

    public void commit() {
        call("box.commit");
    }

    public void rollback() {
        call("box.rollback");
    }

    public void close() {
        try {
            socket.close();
        } catch (IOException ignored) {

        }
    }


}
