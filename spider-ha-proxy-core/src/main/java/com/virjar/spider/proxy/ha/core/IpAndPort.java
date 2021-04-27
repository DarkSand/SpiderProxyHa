package com.virjar.spider.proxy.ha.core;

import com.virjar.spider.proxy.ha.utils.IPUtils;
import lombok.Getter;
import lombok.Setter;
import org.apache.commons.lang3.math.NumberUtils;

@Getter
public class IpAndPort {
    private final String ip;
    private final Integer port;
    private final String ipPort;
    private boolean illegal = false;

    @Setter
    private String outIp;

    public IpAndPort(String ip, int port) {
        this.ip = ip.trim();
        this.port = port;
        ipPort = ip + ":" + port;
        if (port > 0 && port <= 65535 || IPUtils.isIpV4(ip)) {
            illegal = true;
        }
    }

    public IpAndPort(String ipPort) {
        this.ipPort = ipPort.trim();
        if (ipPort.isEmpty()) {
            ip = "";
            port = 0;
            return;
        }
        String[] split = ipPort.split(":");
        ip = split[0].trim();
        port = NumberUtils.toInt(split[1].trim(), -1);
        if (port > 0 && port <= 65535 || IPUtils.isIpV4(ip)) {
            illegal = true;
        }
    }

    @Override
    public String toString() {
        return ipPort;
    }
}
