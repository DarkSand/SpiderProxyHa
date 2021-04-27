package com.virjar.spider.proxy.ha.mocker.handlers;

import com.virjar.spider.proxy.ha.mocker.HttpNettyUtils;
import com.virjar.spider.proxy.ha.mocker.SimpleHttpProxyServer;
import io.netty.channel.*;
import io.netty.handler.codec.http.*;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.math.NumberUtils;

import java.util.List;
import java.util.regex.Pattern;


@Slf4j
public class HttpServerHandler extends SimpleChannelInboundHandler<HttpRequest> {
    private ChannelHandlerContext ctx;
    private HttpRequest httpRequest;
    private boolean isHttps = false;
    private static final int MAX_INITIAL_LINE_LENGTH_DEFAULT = 8192;
    private static final int MAX_HEADER_SIZE_DEFAULT = 8192 * 2;
    private static final int MAX_CHUNK_SIZE_DEFAULT = 8192 * 2;

    @Override
    protected void channelRead0(ChannelHandlerContext ctx, HttpRequest httpRequest) throws Exception {
        this.ctx = ctx;
        if (httpRequest.getDecoderResult().isFailure()) {
            log.warn("Could not parse request from client. Decoder result: {}", httpRequest.getDecoderResult().toString());
            FullHttpResponse response = HttpNettyUtils.createFullHttpResponse(HttpVersion.HTTP_1_1,
                    HttpResponseStatus.BAD_REQUEST,
                    "Unable to parse HTTP request");
            HttpHeaders.setKeepAlive(response, false);
            HttpNettyUtils.respondWithShortCircuitResponse(ctx.channel(), response);
            return;
        }
        this.httpRequest = httpRequest;
        if (isRequestToOriginServer()) {
            HttpNettyUtils.writeBadRequest(ctx.channel(), httpRequest);
            return;
        }
        isHttps = HttpNettyUtils.isCONNECT(httpRequest);

        if (!isHttps) {
            // http代理模式，需要暂停读，否则客户端可能一直发数据过来
            ctx.channel().config().setAutoRead(false);
        }
        String ipAndPort = identifyHostAndPort(httpRequest);
        String[] split = ipAndPort.split(":");
        String targetHost = split[0].trim();
        int port = NumberUtils.toInt(split[1].trim());

        SimpleHttpProxyServer.realBootstrap
                .connect(targetHost, port)
                .addListener(new ChannelFutureListener() {
                    @Override
                    public void operationComplete(ChannelFuture channelFuture) throws Exception {
                        if (!channelFuture.isSuccess()) {
                            // 连接真是服务器失败
                            HttpNettyUtils.writeBadRequest(ctx.channel(), httpRequest);
                            return;
                        }
                        Channel userChannel = ctx.channel();
                        Channel upstreamChannel = channelFuture.channel();

                        HttpNettyUtils.loveOther(userChannel, upstreamChannel);

                        if (isHttps) {
                            HttpResponse response = HttpNettyUtils.createFullHttpResponse(HttpVersion.HTTP_1_1,
                                    CONNECTION_ESTABLISHED);
                            response.headers().set(HttpHeaders.Names.CONNECTION, HttpHeaders.Values.KEEP_ALIVE);
                            HttpNettyUtils.addVia(response, "echo-proxy");
                            ctx.channel().writeAndFlush(response)
                                    .addListener((ChannelFutureListener) channelFuture1 -> {
                                        ChannelPipeline userPipeline = userChannel.pipeline();
                                        userPipeline.remove(HttpResponseEncoder.class);
                                        userPipeline.remove(HttpRequestDecoder.class);
                                        userPipeline.remove(HttpServerHandler.class);

                                        userPipeline.addLast(new RelayHandler(upstreamChannel));
                                        upstreamChannel.pipeline().addLast(new RelayHandler(userChannel));

                                    });
                            return;
                        }
                        // http 模式
                        ctx.channel().config().setAutoRead(true);
                        ChannelPipeline upstreamPipeline = upstreamChannel.pipeline();
                        upstreamPipeline.addFirst(new HttpRequestEncoder());
                        upstreamPipeline.addFirst(new HttpResponseDecoder(MAX_INITIAL_LINE_LENGTH_DEFAULT,
                                MAX_HEADER_SIZE_DEFAULT,
                                MAX_CHUNK_SIZE_DEFAULT));

                        ChannelPipeline userPipeline = userChannel.pipeline();
                        userPipeline.remove(HttpServerHandler.class);

                        userPipeline.addLast(new RelayHandler(upstreamChannel));
                        upstreamChannel.pipeline().addLast(new RelayHandler(userChannel));

                        upstreamChannel.writeAndFlush(httpRequest);

                    }
                });
    }

    // Schemes are case-insensitive:
    // http://tools.ietf.org/html/rfc3986#section-3.1
    private static Pattern HTTP_PREFIX = Pattern.compile("^https?://.*",
            Pattern.CASE_INSENSITIVE);

    private static String parseHostAndPort(final String uri) {
        final String tempUri;
        if (!HTTP_PREFIX.matcher(uri).matches()) {
            // Browsers particularly seem to send requests in this form when
            // they use CONNECT.
            tempUri = uri;
        } else {
            // We can't just take a substring from a hard-coded index because it
            // could be either http or https.
            tempUri = StringUtils.substringAfter(uri, "://");
        }
        final String hostAndPort;
        if (tempUri.contains("/")) {
            hostAndPort = tempUri.substring(0, tempUri.indexOf("/"));
        } else {
            hostAndPort = tempUri;
        }
        return hostAndPort;
    }

    private String identifyHostAndPort(HttpRequest httpRequest) {
        String hostAndPort = parseHostAndPort(httpRequest.getUri());
        if (StringUtils.isBlank(hostAndPort)) {
            List<String> hosts = httpRequest.headers().getAll(
                    HttpHeaders.Names.HOST);
            if (hosts != null && !hosts.isEmpty()) {
                hostAndPort = hosts.get(0);
            }
        }

        if (StringUtils.isBlank(hostAndPort)) {
            return hostAndPort;
        }

        // add add default port config
        // set port=80 for http ;set port=443 for https
        if (hostAndPort.contains(":")) {
            return hostAndPort;
        }

        String uri = httpRequest.getUri();
        if (StringUtils.startsWith(uri, "https:")) {
            return hostAndPort + ":443";
        } else {
            return hostAndPort + ":80";
        }
    }

    private boolean isRequestToOriginServer() {
        if (httpRequest.getMethod() == HttpMethod.CONNECT) {
            return false;
        }

        // direct requests to the proxy have the path only without a scheme
        String uri = httpRequest.getUri();
        return !HTTP_SCHEME.matcher(uri).matches();
    }

    private static final Pattern HTTP_SCHEME = Pattern.compile("^http://.*", Pattern.CASE_INSENSITIVE);

    private static final HttpResponseStatus CONNECTION_ESTABLISHED = new HttpResponseStatus(
            200, "Connection established");
}
