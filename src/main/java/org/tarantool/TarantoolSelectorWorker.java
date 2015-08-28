package org.tarantool;

import java.io.IOException;
import java.nio.channels.ClosedChannelException;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.SocketChannel;
import java.nio.channels.spi.SelectorProvider;
import java.util.AbstractMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingDeque;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

public abstract class TarantoolSelectorWorker implements Runnable {

    public interface ChannelProcessor {

        void read();

        void write();

        void idle();

        void close(Exception e);
    }

    public class Reg {
        SocketChannel channel;
        ChannelProcessor processor;
        BlockingQueue<SelectionKey> result;

        public Reg(SocketChannel channel, ChannelProcessor processor, BlockingQueue<SelectionKey> result) {
            this.channel = channel;
            this.processor = processor;
            this.result = result;
        }
    }

    private final Selector selector;

    private LinkedBlockingQueue<Reg> register = new LinkedBlockingQueue<Reg>();

    public abstract void error(SelectionKey key, Exception e);

    public TarantoolSelectorWorker() throws IOException {
        selector = SelectorProvider.provider().openSelector();
    }

    protected BlockingQueue<SelectionKey> register(SocketChannel channel, ChannelProcessor processor) {
        ArrayBlockingQueue<SelectionKey> queue = new ArrayBlockingQueue<SelectionKey>(1);
        if (register.offer(new Reg(channel, processor, queue))) {
            selector.wakeup();
            return queue;
        }
        return null;
    }

    @Override
    public void run() {
        try {
            try {
                while (!Thread.interrupted()) {
                    if (selector.select() > 0) {
                        Set<SelectionKey> keys = selector.selectedKeys();
                        Iterator<SelectionKey> i = keys.iterator();
                        while (i.hasNext()) {
                            SelectionKey key = i.next();
                            i.remove();
                            if (!key.isValid()) {
                                continue;
                            }
                            ChannelProcessor ps = (ChannelProcessor) key.attachment();
                            try {
                                if (key.isReadable()) {
                                    ps.read();
                                } else if (key.isWritable()) {
                                    ps.write();
                                }
                            } catch (Exception e) {
                                error(key, e);
                                ps.close(e);
                            }

                        }
                    }
                    Set<SelectionKey> keys = selector.keys();
                    for (SelectionKey key : keys) {
                        if (key.isValid()) {
                            ChannelProcessor ps = (ChannelProcessor) key.attachment();
                            try {
                                ps.idle();
                            } catch (Exception e) {
                                error(key, e);
                                ps.close(e);
                            }
                        }
                    }
                    while (!register.isEmpty()) {
                        Reg reg = register.poll();
                        if (reg != null) {
                            try {
                                reg.channel.configureBlocking(false);
                                SelectionKey key = reg.channel.register(selector, SelectionKey.OP_READ, reg.processor);
                                reg.result.offer(key);
                            } catch (Exception e) {
                                reg.processor.close(e);
                            }
                        }

                    }
                }
            } finally {
                selector.close();
            }
        } catch (IOException e) {
            throw new CommunicationException("IO Exception during key selection", e);
        }
    }
}
