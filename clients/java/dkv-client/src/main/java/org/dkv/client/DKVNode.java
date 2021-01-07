package org.dkv.client;

import java.util.Objects;

import static org.dkv.client.Utils.checkf;

/**
 * Represents a DKV endpoint identified by the hostname
 * or IP address and the GRPC port.
 */
public class DKVNode {
    private static final int MAX_PORT_VALUE = 0xFFFF;

    private final String host;
    private final int port;

    public DKVNode(String host, int port) {
        checkf(host != null && !host.trim().isEmpty(), IllegalArgumentException.class, "host must be provided");
        checkf(port > 0 && port <= MAX_PORT_VALUE, IllegalArgumentException.class, "given port %d is invalid", port);

        this.host = host;
        this.port = port;
    }

    public String getHost() {
        return host;
    }

    public int getPort() {
        return port;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        DKVNode dkvNode = (DKVNode) o;
        return port == dkvNode.port && host.equals(dkvNode.host);
    }

    @Override
    public int hashCode() {
        return Objects.hash(host, port);
    }

    // intended for deserialization
    @SuppressWarnings("unused")
    private DKVNode() {
        this.host = null;
        this.port = -1;
    }
}
