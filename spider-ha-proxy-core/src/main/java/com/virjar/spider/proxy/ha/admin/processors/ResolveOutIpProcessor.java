package com.virjar.spider.proxy.ha.admin.processors;

import com.alibaba.fastjson.JSONObject;
import com.virjar.spider.proxy.ha.admin.AdminRequestProcessor;
import com.virjar.spider.proxy.ha.utils.HttpNettyUtils;
import io.netty.channel.Channel;
import io.netty.handler.codec.http.FullHttpResponse;
import io.netty.handler.codec.http.HttpHeaders;
import io.netty.handler.codec.http.HttpResponseStatus;
import io.netty.handler.codec.http.HttpVersion;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;

import java.net.InetSocketAddress;

@Slf4j
public class ResolveOutIpProcessor implements AdminRequestProcessor {
    @Override
    public void process(Channel channel, JSONObject request, HttpHeaders httpHeaders) {
        //可能是ng转发过来，此时带上真实ip
        String remoteAddr = httpHeaders.get("X-Real-IP");
        if (StringUtils.isBlank(remoteAddr)) {
            InetSocketAddress socketAddress = (InetSocketAddress) channel.remoteAddress();
            remoteAddr = socketAddress.getHostString();
        }
        log.info("getPublicIp from:{}", remoteAddr);

        FullHttpResponse fullHttpResponse = HttpNettyUtils.createFullHttpResponse(
                HttpVersion.HTTP_1_1, HttpResponseStatus.OK, remoteAddr
        );
        channel.writeAndFlush(fullHttpResponse);
    }
}
