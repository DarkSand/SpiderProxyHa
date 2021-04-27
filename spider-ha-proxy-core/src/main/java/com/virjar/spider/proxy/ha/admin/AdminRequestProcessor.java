package com.virjar.spider.proxy.ha.admin;

import com.alibaba.fastjson.JSONObject;
import io.netty.channel.Channel;
import io.netty.handler.codec.http.HttpHeaders;

public interface AdminRequestProcessor {
    void process(Channel channel, JSONObject request, HttpHeaders httpHeaders);
}
