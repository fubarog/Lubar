package com.amutter.service.lubar.test.nio;

import java.net.InetSocketAddress;
import java.nio.channels.SocketChannel;

/**
AIO，aio_error()轮训或者回调，具体解释下
 */
public class ReflectionEpollETDemo {

    public static void main(String[] args) throws Exception {
        // 1. 创建一个客户端 Socket 并连接
        SocketChannel channel = SocketChannel.open();
        channel.connect(new InetSocketAddress("localhost", 8080));
        channel.configureBlocking(false);

        // 2. 使用反射将 EPOLLET 标志添加到该 Socket 的 interestOps 中
        enableEpollETByReflection(channel);

        // 3. 注册到 Selector（略）
        System.out.println("EPOLLET 已通过反射设置");
    }

    private static void enableEpollETByReflection(SocketChannel channel) throws Exception {
        // 获取 Channel 内部的 FileDescriptor
        java.lang.reflect.Field fdField = channel.getClass().getSuperclass()
                .getDeclaredField("fd");
        fdField.setAccessible(true);
        java.io.FileDescriptor fd = (java.io.FileDescriptor) fdField.get(channel);

        // 获取 FileDescriptor 中的 int fd 值
        java.lang.reflect.Field fdIntField = java.io.FileDescriptor.class
                .getDeclaredField("fd");
        fdIntField.setAccessible(true);
        int fdVal = fdIntField.getInt(fd);

        // 计算 EPOLLET 标志值：Linux 中 EPOLLET = 1 << 31 = 0x80000000
        int EPOLLET = 0x80000000;

        // 通过反射调用 sun.nio.ch.EPoll 的 setEvent 方法
        Class<?> epollClass = Class.forName("sun.nio.ch.EPoll");
        java.lang.reflect.Method setEventMethod = epollClass.getDeclaredMethod(
                "setEvent", int.class, int.class, int.class);
        setEventMethod.setAccessible(true);

        // 参数：fd, events, epoll 实例（可以是 0 或 epfd，这里占位）
        // 注意：此方法签名在 JDK 9+ 已改变，需要适配
        setEventMethod.invoke(null, fdVal, EPOLLET, 0);

        System.out.println("通过反射设置 EPOLLET 成功，fd=" + fdVal);
    }
}