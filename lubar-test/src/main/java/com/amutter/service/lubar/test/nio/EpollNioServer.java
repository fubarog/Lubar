package com.amutter.service.lubar.test.nio;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;
import java.nio.charset.StandardCharsets;
import java.util.Iterator;
import java.util.Set;

/**
 * 基于 Java NIO（Linux epoll 实现）的 Echo 服务器
 * 单线程 Reactor 模式
 */
public class EpollNioServer {

    private static final int PORT = 8080;
    private static final int BUFFER_SIZE = 1024;

    public static void main(String[] args) throws IOException {
        // 1. 创建 Selector（底层在 Linux 上就是 epoll）
        Selector selector = Selector.open();
        System.out.println("Selector 已创建，底层实现: " + selector.getClass().getName());

        // 2. 创建 ServerSocketChannel（监听端口）
        ServerSocketChannel serverChannel = ServerSocketChannel.open();
        serverChannel.bind(new InetSocketAddress(PORT));
        serverChannel.configureBlocking(false); // 必须设置为非阻塞

        // 3. 将 ServerSocketChannel 注册到 Selector，关注 OP_ACCEPT 事件
        serverChannel.register(selector, SelectionKey.OP_ACCEPT);
        System.out.println("服务器已启动，监听端口: " + PORT);

        // 4. 事件循环（epoll_wait 的 Java 版本）
        while (true) {
            // 阻塞等待就绪事件（对应 epoll_wait）
            int readyCount = selector.select();
            if (readyCount == 0) {
                continue;
            }

            // 获取就绪的事件集合（对应 epoll 返回的就绪列表）
            Set<SelectionKey> selectedKeys = selector.selectedKeys();
            Iterator<SelectionKey> iter = selectedKeys.iterator();

            while (iter.hasNext()) {
                SelectionKey key = iter.next();
                // 处理完后手动移除，否则下次还会保留（LT 模式特性）
                iter.remove();

                try {
                    // 5. 处理新连接（OP_ACCEPT）
                    if (key.isAcceptable()) {
                        handleAccept(key);
                    }
                    // 6. 处理数据可读（OP_READ）
                    else if (key.isReadable()) {
                        handleRead(key);
                    }
                } catch (IOException e) {
                    // 发生异常时，关闭连接并取消注册
                    key.cancel();
                    try {
                        key.channel().close();
                    } catch (IOException ex) {
                        ex.printStackTrace();
                    }
                    System.err.println("连接异常关闭: " + e.getMessage());
                }
            }
        }
    }

    /**
     * 处理新连接（对应 epoll 中的 EPOLLIN 事件，对监听 Socket 来说就是新连接）
     */
    private static void handleAccept(SelectionKey key) throws IOException {
        ServerSocketChannel serverChannel = (ServerSocketChannel) key.channel();
        SocketChannel clientChannel = serverChannel.accept();
        clientChannel.configureBlocking(false);

        // 将新连接注册到 Selector，关注 OP_READ 事件（对应 epoll_ctl 添加 FD）
        clientChannel.register(key.selector(), SelectionKey.OP_READ);
        System.out.println("新客户端连接: " + clientChannel.getRemoteAddress());
    }

    /**
     * 处理数据读取（对应 epoll 中的 EPOLLIN 事件，对普通 Socket 来说就是数据可读）
     */
    private static void handleRead(SelectionKey key) throws IOException {
        SocketChannel clientChannel = (SocketChannel) key.channel();
        ByteBuffer buffer = ByteBuffer.allocate(BUFFER_SIZE);
        int bytesRead = clientChannel.read(buffer);

        // 客户端关闭连接（read 返回 -1）
        if (bytesRead == -1) {
            System.out.println("客户端主动断开: " + clientChannel.getRemoteAddress());
            key.cancel();
            clientChannel.close();
            return;
        }

        if (bytesRead > 0) {
            // 切换为读模式，准备读取数据
            buffer.flip();
            byte[] data = new byte[buffer.remaining()];
            buffer.get(data);
            String msg = new String(data, StandardCharsets.UTF_8).trim();
            System.out.println("收到消息: " + msg + "，来自: " + clientChannel.getRemoteAddress());

            // 回显（Echo）：写回给客户端
            buffer.rewind(); // 重置 position
            clientChannel.write(buffer);
            // 注意：此时 buffer 的数据已发送完毕
        }
        // 如果 bytesRead == 0，说明没有数据可读（非阻塞模式下的正常情况），直接返回
    }
}