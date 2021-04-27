package com.virjar.spider.proxy.ha.mocker;

import com.virjar.spider.proxy.ha.mocker.handlers.HttpServerHandler;
import io.netty.bootstrap.Bootstrap;
import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.*;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.channel.socket.nio.NioSocketChannel;
import io.netty.handler.codec.http.HttpRequestDecoder;
import io.netty.handler.codec.http.HttpResponseEncoder;
import io.netty.handler.timeout.IdleStateHandler;
import io.netty.util.concurrent.DefaultThreadFactory;

public class SimpleHttpProxyServer {
    private static final ServerBootstrap httpProxyBootstrap;
    public static final Bootstrap realBootstrap;
    private static final int MAX_INITIAL_LINE_LENGTH_DEFAULT = 8192;
    private static final int MAX_HEADER_SIZE_DEFAULT = 8192 * 2;
    private static final int MAX_CHUNK_SIZE_DEFAULT = 8192 * 2;

    private int proxyServerPort;
    private Channel serverChannel;

    public SimpleHttpProxyServer(int proxyServerPort) {
        this.proxyServerPort = proxyServerPort;
        httpProxyBootstrap.bind(proxyServerPort)
                .addListener((ChannelFutureListener) channelFuture -> {
                    if (channelFuture.isSuccess()) {
                        serverChannel = channelFuture.channel();
                    } else {
                        System.out.println("error to open proxy server:");
                        channelFuture.cause().printStackTrace();
                    }
                });
    }

    static {
        httpProxyBootstrap = new ServerBootstrap();
        NioEventLoopGroup serverBossGroup = new NioEventLoopGroup(
                0,
                new DefaultThreadFactory("HttpProxy-boss-group" + DefaultThreadFactory.toPoolName(NioEventLoopGroup.class))
        );
        NioEventLoopGroup serverWorkerGroup = new NioEventLoopGroup(
                0,
                new DefaultThreadFactory("HttpProxy-worker-group" + DefaultThreadFactory.toPoolName(NioEventLoopGroup.class))
        );
        httpProxyBootstrap.group(serverBossGroup, serverWorkerGroup)
                .channel(NioServerSocketChannel.class)
                .childHandler(new ChannelInitializer<SocketChannel>() {
                    @Override
                    public void initChannel(SocketChannel ch) {
                        ChannelPipeline pipeline = ch.pipeline();
                        pipeline.addLast(
                                "idle",
                                new IdleStateHandler(0, 0, 70)
                        );
                        // 需要支持socks4/socks5/http/https
                        // 所以这里需要判定协议类型
                        pipeline.addLast(new HttpResponseEncoder());
                        pipeline.addLast(new HttpRequestDecoder(
                                MAX_INITIAL_LINE_LENGTH_DEFAULT,
                                MAX_HEADER_SIZE_DEFAULT,
                                MAX_CHUNK_SIZE_DEFAULT));
                        pipeline.addLast(new HttpServerHandler());
                    }
                });

        realBootstrap = new Bootstrap();
        realBootstrap.handler(new StubHandler())
                .channelFactory(NioSocketChannel::new).
                group(new NioEventLoopGroup(
                        0,
                        new DefaultThreadFactory("real-request-group" + DefaultThreadFactory.toPoolName(NioEventLoopGroup.class))
                ));
    }

    @ChannelHandler.Sharable
    public static class StubHandler extends ChannelInboundHandlerAdapter {

        @Override
        public void channelRead(ChannelHandlerContext ctx, Object msg) throws Exception {
            super.channelRead(ctx, msg);
            // 这个代码应该不会发生
            // ReferenceCountUtil.release(msg);
        }


    }
}
