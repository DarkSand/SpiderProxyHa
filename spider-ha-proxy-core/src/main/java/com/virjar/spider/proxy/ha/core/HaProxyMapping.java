package com.virjar.spider.proxy.ha.core;

import com.virjar.spider.proxy.ha.Configs;
import com.virjar.spider.proxy.ha.handlers.ProxyProtocolRouter;
import com.virjar.spider.proxy.ha.safethread.ValueCallback;
import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelPipeline;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.handler.timeout.IdleStateHandler;
import io.netty.util.AttributeKey;
import io.netty.util.concurrent.DefaultThreadFactory;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class HaProxyMapping {
    private static ServerBootstrap httpProxyBootstrap;
    @Getter
    private final Integer localMappingPort;
    @Getter
    private Upstream upstream;
    @Getter
    private final Source source;
    private Channel serverChannel;

    private static final AttributeKey<HaProxyMapping> proxyMappingKey = AttributeKey.newInstance("haProxyMapping");

    public HaProxyMapping(Integer localMappingPort, Upstream upstream, Source source) {
        this.localMappingPort = localMappingPort;
        this.upstream = upstream;
        this.source = source;
    }

    private void onProxyServerEstablish(Channel channel) {
        // 服务端的channel
        channel.attr(proxyMappingKey).set(this);
        serverChannel = channel;

        upstream.addDestroyListener(upstream -> {
            log.warn("log...");
        });

    }

    public static HaProxyMapping get(Channel channel) {
        if (channel == null) {
            return null;
        }
        HaProxyMapping haProxyMapping = channel.attr(proxyMappingKey).get();
        if (haProxyMapping != null) {
            return haProxyMapping;
        }
        return get(channel.parent());
    }


    public void startMapping() {
        log.info("startMapping from {}:{}", Configs.listenIp, localMappingPort);
        httpProxyBootstrap.bind(Configs.listenIp, localMappingPort)
                .addListener((ChannelFutureListener) channelFuture -> {
                    if (!channelFuture.isSuccess()) {
                        log.error("can not open proxy server on port:{}", localMappingPort, channelFuture.cause());
                        source.getLooper().postDelay(new Runnable() {
                            @Override
                            public void run() {
                                // 本地端口开启失败，大概率是业务上其他业务占用了端口
                                // 比如连接保持，所以这里5s后再重试开启
                                source.onMappingLose(HaProxyMapping.this);
                            }
                        }, 5000);

                        return;
                    }

                    Channel channel = channelFuture.channel();
                    onProxyServerEstablish(channel);
                });
        upstream.become(UpstreamStat.MAPPING);
    }

    void routeUpstream(Upstream newUpstream) {
        newUpstream.become(UpstreamStat.MAPPING);
        this.upstream.become(UpstreamStat.DESTROYED);
        this.upstream = newUpstream;
    }

    public static void staticInit() {
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
                        pipeline.addLast(new ProxyProtocolRouter());
                    }
                });


    }


    public String resourceKey() {
        return upstream.resourceKey();
    }


    public void doClose() {
        serverChannel.close().addListener(
                // 完成之后再回调，要不然可能出问题
                future -> source.onMappingLose(HaProxyMapping.this)
        );

    }


    public void borrowConnect(ValueCallback<Channel> valueCallback) {
        upstream.borrowConnect(value -> {
            if (value != null) {
                valueCallback.onReceiveValue(value);
                return;
            }
            // 只要有空闲代理资源，那么代理connect永远不会失败
            // 之后就是如何处理优化延时问题了
            failover(valueCallback);
        });
    }

    public boolean isActive() {
        Upstream upstream = this.upstream;
        return upstream != null && upstream.isActive();
    }

    private void failover(ValueCallback<Channel> valueCallback) {
        source.failover(this, valueCallback);
    }
}
