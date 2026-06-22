package com.bnpparibas.dec.bookingconfirmation.domain.model;

import java.net.InetAddress;
import java.net.UnknownHostException;

/**
 * Unique identity of this JVM instance ({@code hostname-pid}), used as the owner token for
 * distributed locks so peers can tell which instance holds a region's lock.
 */
public record InstanceId(String value) {

    public static InstanceId generate() {
        String host;
        try {
            host = InetAddress.getLocalHost().getHostName();
        } catch (final UnknownHostException e) {
            host = "unknown-host";
        }
        return new InstanceId(host + "-" + ProcessHandle.current().pid());
    }

    @Override
    public String toString() {
        return value;
    }
}
