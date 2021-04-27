package com.virjar.spider.proxy.ha.admin;

import com.alibaba.fastjson.JSONException;
import com.alibaba.fastjson.JSONObject;
import com.virjar.spider.proxy.ha.admin.netty.ContentType;
import com.virjar.spider.proxy.ha.admin.netty.DefaultHtmlHttpResponse;
import com.virjar.spider.proxy.ha.admin.netty.Multimap;
import com.virjar.spider.proxy.ha.utils.HttpNettyUtils;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.handler.codec.http.*;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;

import java.net.URI;
import java.net.URISyntaxException;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;

@Slf4j
public class PortalHttpRequestDispatcher extends SimpleChannelInboundHandler<FullHttpRequest> {
    private ContentType contentType;
    private HttpMethod method;
    private String query;

    @Override
    protected void channelRead0(ChannelHandlerContext channelHandlerContext, FullHttpRequest request) throws Exception {
        if (request.getDecoderResult().isFailure()) {
            FullHttpResponse response = HttpNettyUtils.createFullHttpResponse(HttpVersion.HTTP_1_1,
                    HttpResponseStatus.BAD_REQUEST,
                    "Unable to parse HTTP request");
            HttpHeaders.setKeepAlive(response, false);
            log.warn("Could not parse request from client. Decoder result: {}", request.getDecoderResult().toString());
            HttpNettyUtils.respondWithShortCircuitResponse(channelHandlerContext.channel(), response);
            return;
        }
        URI uri;
        try {
            uri = new URI(request.getUri());
        } catch (URISyntaxException e) {
            Channel channel = channelHandlerContext.channel();
            channel.writeAndFlush(DefaultHtmlHttpResponse.badRequest()).addListener(ChannelFutureListener.CLOSE);
            return;
        }
        method = request.getMethod();
        String urlPath = uri.getPath();
        query = uri.getQuery();
        parseContentType(channelHandlerContext, request);
        //application/x-www-form-urlencoded
        //application/json

        if (!"application/x-www-form-urlencoded".equalsIgnoreCase(contentType.getMimeType())
                && !"application/json".equalsIgnoreCase(contentType.getMimeType())) {
            String errorMessage = "spider proxy ha framework only support contentType:application/x-www-form-urlencoded | application/json, now is: " + contentType.getMimeType();
            DefaultHtmlHttpResponse contentTypeNotSupportMessage = new DefaultHtmlHttpResponse(errorMessage);

            Channel channel = channelHandlerContext.channel();
            channel.writeAndFlush(contentTypeNotSupportMessage).addListener(ChannelFutureListener.CLOSE);
            return;
        }

        JSONObject jsonObject = buildRequestJson(request);
        HttpHeaders headers = request.headers();
        PortalManager.handleRequest(urlPath,channelHandlerContext.channel(), jsonObject, headers);
    }

    private JSONObject buildRequestJson(FullHttpRequest request) {
        //now build request
        JSONObject requestJson = new JSONObject();
        if (StringUtils.isNotBlank(query)) {
            for (Map.Entry<String, List<String>> entry : Multimap.parseUrlEncoded(query).entrySet()) {
                if (entry.getValue() == null || entry.getValue().size() == 0) {
                    continue;
                }
                requestJson.put(entry.getKey(), entry.getValue().get(0));
            }
        }

        if (method.equals(HttpMethod.POST)) {
            String charset = contentType.getCharset();
            if (charset == null) {
                charset = StandardCharsets.UTF_8.name();
            }
            String postBody = request.content().toString(Charset.forName(charset));
            try {
                requestJson.putAll(JSONObject.parseObject(postBody));
            } catch (JSONException e) {
                for (Map.Entry<String, List<String>> entry : Multimap.parseUrlEncoded(postBody).entrySet()) {
                    if (entry.getValue() == null || entry.getValue().size() == 0) {
                        continue;
                    }
                    requestJson.put(entry.getKey(), entry.getValue().get(0));
                }
            }
        }
        return requestJson;
    }

    private void parseContentType(ChannelHandlerContext channelHandlerContext, FullHttpRequest request) {
        //create a request
        contentType = ContentType.from(request.headers().get("Content-Type"));
        if (contentType == null && !method.equals(HttpMethod.GET)) {
            //不识别的请求类型
            Channel channel = channelHandlerContext.channel();
            channel.writeAndFlush(DefaultHtmlHttpResponse.badRequest()).addListener(ChannelFutureListener.CLOSE);
            return;
        }
        if (contentType == null) {
            contentType = ContentType.from("application/x-www-form-urlencoded;charset=utf8");
        }
    }
}
