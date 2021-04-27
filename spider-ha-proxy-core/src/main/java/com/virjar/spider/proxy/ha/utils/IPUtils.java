package com.virjar.spider.proxy.ha.utils;

import org.apache.commons.lang3.math.NumberUtils;

import java.net.*;
import java.util.Enumeration;

public class IPUtils {
    public static boolean isIpV4(String input) {
        // 3 * 4 + 3 = 15
        if (input.length() > 15) {
            return false;
        }
        String[] split = input.split("\\.");
        if (split.length != 4) {
            return false;
        }
        for (String segment : split) {
            int i = NumberUtils.toInt(segment, -1);
            if (i < 0 || i > 255) {
                return false;
            }
        }
        return true;
    }

    public static boolean isPrivate(String input) {
        return input.startsWith("192.")
                || input.startsWith("10.")
                || input.startsWith("172.") || input.equals("127.0.0.1");
    }

    public static String fetchIp(String type) throws SocketException {
        Enumeration<NetworkInterface> networkInterfaces = NetworkInterface.getNetworkInterfaces();
        while (networkInterfaces.hasMoreElements()) {
            NetworkInterface networkInterface = networkInterfaces.nextElement();
            if (networkInterface.isLoopback()) {
                continue;
            }

            Enumeration<InetAddress> inetAddresses = networkInterface.getInetAddresses();
            while (inetAddresses.hasMoreElements()) {
                InetAddress inetAddress = inetAddresses.nextElement();
                if (inetAddress instanceof Inet6Address) {
                    continue;
                }
                Inet4Address inet4Address = (Inet4Address) inetAddress;
                byte[] address = inet4Address.getAddress();
                if (address.length != 4) {
                    continue;
                }
                int firstByte = address[0] & 0xFF;
                boolean isPrivate = (firstByte == 192 || firstByte == 10 || firstByte == 172);
                if (type.equals("private")) {
                    if (isPrivate) {
                        return inet4Address.getHostAddress();
                    }
                } else {
                    if (!isPrivate) {
                        return inet4Address.getHostAddress();
                    }
                }
            }
        }
        return null;
    }

}
