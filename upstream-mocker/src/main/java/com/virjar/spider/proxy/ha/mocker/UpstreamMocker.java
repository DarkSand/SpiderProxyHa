package com.virjar.spider.proxy.ha.mocker;

import fi.iki.elonen.NanoHTTPD;

import java.io.IOException;

public class UpstreamMocker {
    public static void main(String[] args) throws IOException {
        int start = 26980;
        int proxySize = 10;
        StringBuilder sb = new StringBuilder();
        for (int i = start; i < start + proxySize; i++) {
            new SimpleHttpProxyServer(i);
            sb.append("127.0.0.1:").append(i).append("\n");
        }
        String proxyServerList = sb.toString();


        new NanoHTTPD(8080) {
            @Override
            public Response serve(IHTTPSession session) {
                return newFixedLengthResponse(Response.Status.OK, NanoHTTPD.MIME_PLAINTEXT, proxyServerList);
            }
        }.start();
    }
}
