package org.dkv.client;

import java.util.Objects;

import static org.dkv.client.Utils.checkf;

public class DKVShard {
    private static final int MAX_PORT_VALUE = 0xFFFF;

    private final String host;
    private final int port;
    private final String authority;

    public DKVShard(String host, int port, String authority) {
        checkf(host != null && !host.trim().isEmpty(), IllegalArgumentException.class, "host must be provided");
        checkf(port > 0 && port <= MAX_PORT_VALUE, IllegalArgumentException.class, "given port %d is invalid", port);
        checkf(authority != null && !authority.trim().isEmpty(), IllegalArgumentException.class, "authority must be provided");

        this.host = host;
        this.port = port;
        this.authority = authority;
    }

    public String getHost() {
        return host;
    }

    public int getPort() {
        return port;
    }

    public String getAuthority() {
        return authority;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        DKVShard dkvShard = (DKVShard) o;
        return port == dkvShard.port &&
                host.equals(dkvShard.host) &&
                authority.equals(dkvShard.authority);
    }

    @Override
    public int hashCode() {
        return Objects.hash(host, port, authority);
    }

    @Override
    public String toString() {
        return "DKVShard{" +
                "host='" + host + '\'' +
                ", port=" + port +
                ", authority='" + authority + '\'' +
                '}';
    }
}
