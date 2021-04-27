package com.virjar.spider.proxy.ha;

import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.virjar.spider.proxy.ha.core.Source;
import io.netty.util.internal.ConcurrentSet;

import java.util.List;
import java.util.Map;
import java.util.Set;

public class Configs {
    public static List<Source> sourceList;
    /**
     * 资源刷新间隔时间
     */
    public static int refreshUpstreamInterval = 30;
    /**
     * 后端探测接口，探测代理ip是否可用以及解析出口ip地址
     */
    public static String proxyHttpTestURL = "https://sekiro.virjar.com/dly/getPublicIp";

    public static int cacheConnPerUpstream = 3;
    public static int cacheConnAliveSeconds = 30;

    public static String listenIp = "0.0.0.0";

    public static int adminServerPort = 9085;
    public static String adminApiToken = "";

    public static void doRefreshResource() {
        for (Source source : sourceList) {
            source.refresh();
        }
    }

    public static Set<String> openPortSet = Sets.newConcurrentHashSet();

    public static Map<Integer, Source> sourceMap = Maps.newHashMap();
}
