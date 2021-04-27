package com.virjar.spider.proxy.ha.core;

/**
 * 代理ip资源状态
 */
public enum UpstreamStat {
    // 被mapping,为一一对应
    MAPPING,
    // 空转，代理ip资源数量大于mapping space的时候，有部分代理会处于空转状态
    IDLE,
    // 本代理ip被销毁，在SpiderProxyHa层面判定失效。或者API调用使他下线
    DESTROYED
}
