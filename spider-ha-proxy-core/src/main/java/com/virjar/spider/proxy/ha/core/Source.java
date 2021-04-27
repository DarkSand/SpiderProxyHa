package com.virjar.spider.proxy.ha.core;

import com.google.common.base.Splitter;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.virjar.spider.proxy.ha.Configs;
import com.virjar.spider.proxy.ha.safethread.Looper;
import com.virjar.spider.proxy.ha.safethread.ValueCallback;
import io.netty.channel.Channel;
import lombok.Getter;
import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;

import java.util.*;

/**
 * 数据源，通过特定url加载代理资源列表，HA资源路由过程发生在固定source下，不会发生source之间的资源飘逸
 */
@Slf4j
@RequiredArgsConstructor
public class Source {
    @NonNull
    @Getter
    private final String name;
    @NonNull
    @Getter
    private final String protocol;
    @NonNull
    @Getter
    private final String sourceUrl;
    @NonNull
    private final String mappingSpace;
    @Setter
    @Getter
    private String upstreamAuthUser;
    @Setter
    @Getter
    private String upstreamAuthPassword;

    private final TreeSet<Integer> needBindPort = new TreeSet<>();
    private int portMappingSize;

    private final LinkedList<Upstream> availableUpstream = new LinkedList<>();
    //代理ip资源，验证通过，但是当前系统没有使用，缓存起来
    private final LinkedList<IpAndPort> availableIpAndPort = Lists.newLinkedList();
    private final UpstreamProducer upstreamProducer = new UpstreamProducer(this);


//    private final Cache<String, String> outIpResource = CacheBuilder.newBuilder()
//            .expireAfterWrite(10, TimeUnit.MINUTES)
//            .build();

    @Getter
    private Looper looper;
    private boolean init = false;


    private final TreeMap<Integer, HaProxyMapping> mapping = Maps.newTreeMap();


    private void doInit() {
        init = true;
        parseConfig();
        looper = new Looper("main-" + name);
    }

    public void refresh() {
        if (!init) {
            doInit();
        }
        upstreamProducer.refresh();
    }


    void handleUpstreamResource(IpAndPort ipAndPort) {
        if (!looper.inLooper()) {
            looper.post(() -> handleUpstreamResource(ipAndPort));
            return;
        }

        Integer localMappingPort = needBindPort.pollFirst();
        if (localMappingPort != null) {
            Upstream upstream = new Upstream(this, ipAndPort);
            HaProxyMapping haProxyMapping = new HaProxyMapping(localMappingPort, upstream, this);
            this.mapping.put(localMappingPort, haProxyMapping);
            haProxyMapping.startMapping();
            return;
        }

        if (availableUpstream.size() > portMappingSize * 2) {
            // 两级缓存，太多的可用ip了，所以这里放到内存，不进行连接创建
            availableIpAndPort.addFirst(ipAndPort);
            return;
        }

        Upstream upstream = new Upstream(this, ipAndPort);
        availableUpstream.addFirst(upstream);


        upstream.addDestroyListener(upstream1 -> {
            if (availableIpAndPort.isEmpty() && !upstream1.isBlack()) {
                // 有可能有误判，所以如果ip不可用了那么重新再探测下
                // 但是如果代理资源足够，那么我们也放弃探测
                upstreamProducer.reTestUpstream(upstream1);
            }
        });
    }

    private void parseConfig() {
        Iterable<String> pairs = Splitter.on(":").split(mappingSpace);
        for (String pair : pairs) {
            if (pair.contains("-")) {
                int index = pair.indexOf("-");
                String startStr = pair.substring(0, index);
                String endStr = pair.substring(index + 1);
                int start = Integer.parseInt(startStr);
                int end = Integer.parseInt(endStr);
                for (int i = start; i <= end; i++) {
                    needBindPort.add(i);
                }
            } else {
                needBindPort.add(Integer.parseInt(pair));
            }
        }
        portMappingSize = needBindPort.size();
        for (Integer port : needBindPort) {
            if (!Configs.openPortSet.add(Configs.listenIp + ":" + port)) {
                throw new IllegalStateException("duplicate port config :" + port);
            }
            Configs.sourceMap.put(port, this);
        }

    }


    public void onMappingLose(HaProxyMapping haProxyMapping) {
        looper.post(() -> {
            // 只有本地端口无法打开的时候，才会close mapping
            log.info("return local mapping :{}", haProxyMapping.getLocalMappingPort());
            needBindPort.add(haProxyMapping.getLocalMappingPort());
            mapping.remove(haProxyMapping.getLocalMappingPort());
        });
    }

    public boolean needAuth() {
        return StringUtils.isNotBlank(upstreamAuthUser);
    }


    // HaProxyMapping 所绑定的upstream
    public void failover(HaProxyMapping haProxyMapping, ValueCallback<Channel> valueCallback) {
        if (!looper.inLooper()) {
            looper.post(() -> failover(haProxyMapping, valueCallback));
            return;
        }

        if (failoverWithIdleConnection(haProxyMapping, valueCallback)) {
            // 使用空转的ip资源，并成功路由
            return;
        }

        expandIdleUpstream();

        if (availableUpstream.isEmpty()) {
            // 没有空闲的ip资源，使用当前其他绑定的ip资源
            failoverWithBindingUpstream(haProxyMapping, valueCallback);
        } else {
            failoverWithUnbindingUpstream(haProxyMapping, valueCallback);
        }

    }

    private void expandIdleUpstream() {
        looper.checkLooper();
        // 扩展空转代理ip资源
        while (true) {
            if (availableUpstream.size() >= portMappingSize * 2) {
                return;
            }

            IpAndPort ipAndPort = availableIpAndPort.pollFirst();
            if (ipAndPort == null) {
                return;
            }

            Upstream upstream = new Upstream(this, ipAndPort);
            // 比较重要，由于可能存在探测时间距离使用时间过长，所以这批资源放到尾部，减少使用概率
            availableUpstream.addLast(upstream);
        }
    }

    private void failoverWithUnbindingUpstream(HaProxyMapping haProxyMapping, ValueCallback<Channel> valueCallback) {
        looper.checkLooper();
        // 在 availableUpstream中挑选合适的代理ip，创建代理请求
        // 选择3个代理ip，并同时发送连接建立请求
        // 先创建成功的，作为最终的路由成功的ip，并且完成当前mapping到当前upstream的绑定
        // 对于非第一个创建成功的ip，缓存连接对象，等待下次代理请求分发，而不进行销毁
        // 如果所有ip都失败，那么最终返回失败

        UnBindingFailOverValueCallbackHandler unBindingFailOverValueCallback
                = new UnBindingFailOverValueCallbackHandler(haProxyMapping, valueCallback);

        int retryUpstream = 0;

        while (true) {
            if (availableUpstream.isEmpty()) {
                break;
            }
            Upstream upstream = availableUpstream.poll();
            if (!upstream.isActive()) {
                continue;
            }
            retryUpstream++;
            unBindingFailOverValueCallback.addTestUpstream(upstream);
            // 这里其实是并发的往不同的upstream创建连接
            upstream.createUpstreamNoCache(new UpstreamTaskFailOverValueCallback(unBindingFailOverValueCallback, upstream));
            if (retryUpstream > 3) {
                break;
            }
        }

        if (retryUpstream == 0) {
            // 没有获取到任何ip资源，理论上这个不会发生
            valueCallback.onReceiveValue(null);
        }
    }

    private class UpstreamTaskFailOverValueCallback implements ValueCallback<Channel> {
        private final UnBindingFailOverValueCallbackHandler unBindingFailOverValueCallback;
        private final Upstream upstream;

        UpstreamTaskFailOverValueCallback(UnBindingFailOverValueCallbackHandler unBindingFailOverValueCallback, Upstream upstream) {
            this.unBindingFailOverValueCallback = unBindingFailOverValueCallback;
            this.upstream = upstream;
        }

        @Override
        public void onReceiveValue(Channel value) {
            unBindingFailOverValueCallback.handleCallback(value, upstream);
        }
    }

    private class UnBindingFailOverValueCallbackHandler {
        private final HaProxyMapping haProxyMapping;
        private final ValueCallback<Channel> valueCallback;
        private final List<Upstream> testUpstream = Lists.newArrayListWithCapacity(3);
        private boolean success = false;

        void addTestUpstream(Upstream upstream) {
            testUpstream.add(upstream);
        }

        UnBindingFailOverValueCallbackHandler(HaProxyMapping haProxyMapping, ValueCallback<Channel> valueCallback) {
            this.haProxyMapping = haProxyMapping;
            this.valueCallback = valueCallback;
        }

        private void handleCallback(Channel value, Upstream upstream) {
            looper.post(() -> handleCallbackSafe(value, upstream));
        }

        private void handleCallbackSafe(Channel value, Upstream upstream) {
            testUpstream.remove(upstream);
            if (value == null) {
                // 空转的资源，如果失败了，就直接销毁
                upstream.become(UpstreamStat.DESTROYED);
                if (testUpstream.size() == 0 && !success) {
                    // 最后一个都没有成功
                    valueCallback.onReceiveValue(null);
                }
                return;
            }
            if (!success) {
                success = true;
                // 第一个完成连接，进行绑定
                haProxyMapping.routeUpstream(upstream);
                valueCallback.onReceiveValue(value);
                return;
            }
            // 成功完成连接，但是不是第一个，所以进行连接缓存
            upstream.reCacheChannel(value);
        }

    }

    private void failoverWithBindingUpstream(HaProxyMapping haProxyMapping, ValueCallback<Channel> valueCallback) {
        looper.checkLooper();
        log.warn("failoverWithBindingUpstream from local port:{}", haProxyMapping.getLocalMappingPort());
        // 已经没有空转的代理ip资源用于路由了，此时只是临时选择一个已经再使用的ip
        // 这样目的是尽可能保证ip请求不失败，
        // 正常情况逻辑不应该走到这里，走到这里证明配置不合理
        // 本流程不会进行ip绑定关系路由
        // 同时，为了保证failOver并不会太随机，本流程进行一致性hash原则，顺序路由
        if (mapping.size() <= 1) {
            // 因为自己也在里面，所以1代表没有一个可用的
            valueCallback.onReceiveValue(null);
            return;
        }
        int nowTestPort = haProxyMapping.getLocalMappingPort() + 1;
        int maxRetry = Math.min(mapping.size(), 10);

        HaProxyMapping failoverHaProxyMappingResource = null;

        for (int retryIndex = 0; retryIndex < maxRetry; retryIndex++) {
            // 注意下 tailMap左侧闭合，包括自身
            SortedMap<Integer, HaProxyMapping> sortedMap = mapping.tailMap(nowTestPort);
            if (sortedMap.isEmpty()) {
                sortedMap = mapping;
            }
            HaProxyMapping next = sortedMap.values().iterator().next();
            nowTestPort = next.getLocalMappingPort() + 1;
            if (next.getLocalMappingPort().equals(haProxyMapping.getLocalMappingPort())) {
                // 命中了自己，进入到failover流程，那么自己肯定是不可用的了
                continue;
            }
            if (!next.isActive()) {
                continue;
            }
            failoverHaProxyMappingResource = next;
            break;
        }
        if (failoverHaProxyMappingResource == null) {
            valueCallback.onReceiveValue(null);
            return;
        }
        failoverHaProxyMappingResource.getUpstream().borrowConnect(valueCallback);
    }

    private boolean failoverWithIdleConnection(HaProxyMapping haProxyMapping, ValueCallback<Channel> valueCallback) {
        Upstream routedUpstream = null;
        Channel cacheChannelFromRouteUpstream = null;
        LinkedList<Upstream> tempLinkedList = Lists.newLinkedList();
        while (true) {
            Upstream tempUpstream = availableUpstream.poll();
            if (tempUpstream == null) {
                break;
            }
            if (!tempUpstream.isActive()) {
                continue;
            }

            Channel channel = tempUpstream.pollCacheChannel();
            if (channel == null) {
                tempLinkedList.addFirst(tempUpstream);
                continue;
            }
            routedUpstream = tempUpstream;
            cacheChannelFromRouteUpstream = channel;
            break;
        }
        availableUpstream.addAll(tempLinkedList);
        if (cacheChannelFromRouteUpstream != null) {
            haProxyMapping.routeUpstream(routedUpstream);
            valueCallback.onReceiveValue(cacheChannelFromRouteUpstream);
            return true;
        }
        return false;
    }

    public void reDial(Integer port) {
        HaProxyMapping haProxyMapping = mapping.get(port);
        if (haProxyMapping == null) {
            log.warn("can not find haProxyMapping for mapping :{}", port);
            return;
        }
        Upstream upstream = haProxyMapping.getUpstream();
        upstream.setBlack(true);
        looper.post(() -> upstream.become(UpstreamStat.DESTROYED));
    }
}
