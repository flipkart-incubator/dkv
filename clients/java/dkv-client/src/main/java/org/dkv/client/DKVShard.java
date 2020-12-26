package org.dkv.client;

import java.util.Objects;

import static org.dkv.client.Utils.checkf;

public class DKVShard {
    private static final int MAX_PORT_VALUE = 0xFFFF;

    private final String name;
    private final String host;
    private final int port;

    public DKVShard(String name, String host, int port) {
        checkf(name != null && !name.trim().isEmpty(), IllegalArgumentException.class, "shard name must be provided");
        checkf(host != null && !host.trim().isEmpty(), IllegalArgumentException.class, "host must be provided");
        checkf(port > 0 && port <= MAX_PORT_VALUE, IllegalArgumentException.class, "given port %d is invalid", port);

        this.name = name;
        this.host = host;
        this.port = port;
    }

    public String getHost() {
        return host;
    }

    public int getPort() {
        return port;
    }

    public String getName() {
        return name;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        DKVShard dkvShard = (DKVShard) o;
        return port == dkvShard.port &&
                host.equals(dkvShard.host) &&
                name.equals(dkvShard.name);
    }

    @Override
    public int hashCode() {
        return Objects.hash(host, port, name);
    }

    @Override
    public String toString() {
        return "DKVShard{" +
                "host='" + host + '\'' +
                ", port=" + port +
                ", authority='" + name + '\'' +
                '}';
    }

    // intended for deserialization
    @SuppressWarnings("unused")
    private DKVShard() {
        this.name = null;
        this.host = null;
        this.port = -1;
    }
}
