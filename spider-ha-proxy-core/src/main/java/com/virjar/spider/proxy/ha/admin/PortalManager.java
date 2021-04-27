package com.virjar.spider.proxy.ha.admin;

import com.alibaba.fastjson.JSONObject;
import com.google.common.collect.Maps;
import com.virjar.spider.proxy.ha.Configs;
import com.virjar.spider.proxy.ha.Constants;
import com.virjar.spider.proxy.ha.admin.processors.ReDialProcessor;
import com.virjar.spider.proxy.ha.admin.processors.ResolveOutIpProcessor;
import com.virjar.spider.proxy.ha.utils.HttpNettyUtils;
import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.*;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.handler.codec.http.*;
import io.netty.handler.timeout.IdleStateHandler;
import io.netty.util.concurrent.DefaultThreadFactory;
import lombok.extern.slf4j.Slf4j;

import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * 管理端，可以用于操作代理服务器<br>
 * 但是只提供api，不考虑提供web页面
 */
@Slf4j
public class PortalManager {
    private static final AtomicBoolean started = new AtomicBoolean(false);

    public static void startService() {
        if (started.compareAndSet(false, true)) {
            startServiceInternal();
            registerHandler(Constants.ADMIN_API_PATH.RESOLVE_IP, new ResolveOutIpProcessor());
            registerHandler(Constants.ADMIN_API_PATH.RE_DIAL, new ReDialProcessor());
        }
    }

    public static void registerHandler(String path, AdminRequestProcessor adminRequestProcessor) {
        path = path.trim();
        if (!path.startsWith("/")) {
            path = "/" + path;
        }
        processors.put(path, adminRequestProcessor);
    }

    public static Map<String, AdminRequestProcessor> processors = Maps.newConcurrentMap();


    public static void handleRequest(String urlPath, Channel channel, JSONObject request, HttpHeaders httpHeaders) {
        AdminRequestProcessor processor = processors.get(urlPath);
        if (processor == null) {
            String body = "Bad Request to URI: " + urlPath;
            FullHttpResponse response = HttpNettyUtils.createFullHttpResponse(HttpVersion.HTTP_1_1, HttpResponseStatus.BAD_REQUEST, body);
            channel.writeAndFlush(response).addListener(ChannelFutureListener.CLOSE);
            return;
        }
        processor.process(channel, request, httpHeaders);
    }

    private static void startServiceInternal() {
        ServerBootstrap serverBootstrap = new ServerBootstrap();
        NioEventLoopGroup serverBossGroup = new NioEventLoopGroup(
                0,
                new DefaultThreadFactory("admin-boss-group" + DefaultThreadFactory.toPoolName(NioEventLoopGroup.class))
        );
        NioEventLoopGroup serverWorkerGroup = new NioEventLoopGroup(
                0,
                new DefaultThreadFactory("admin-worker-group" + DefaultThreadFactory.toPoolName(NioEventLoopGroup.class))
        );
        serverBootstrap.group(serverBossGroup, serverWorkerGroup)
                .channel(NioServerSocketChannel.class)
                .childHandler(new ChannelInitializer<SocketChannel>() {
                    @Override
                    public void initChannel(SocketChannel ch) {
                        ChannelPipeline pipeline = ch.pipeline();
                        pipeline.addLast(
                                "idle",
                                new IdleStateHandler(0, 0, 70)
                        );
                        pipeline.addLast(new HttpServerCodec());
                        // 作为api功能，不支持文件上传之类的大请求，所以只提供5k
                        pipeline.addLast(new HttpObjectAggregator(5 * 1025));
                        pipeline.addLast(new HttpContentCompressor());
                        pipeline.addLast(new PortalHttpRequestDispatcher());
                    }
                });
        serverBootstrap.bind(Configs.adminServerPort)
                .addListener((ChannelFutureListener) future -> {
                    if (!future.isSuccess()) {
                        log.error("error open admin service", future.cause());

                    }
                });
    }
}
