package org.dkv.client;

import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;

import static org.dkv.client.Utils.checkf;

/**
 * Represents a set of DKV nodes identified by a name. It is
 * intended for use cases where the keyspace is sharded across
 * multiple DKV clusters. In such setups, the set of DKV masters
 * are captured within a DKVNodeSet instance while the
 * set of DKV slaves are captured in a separate DKVNodeSet
 * instance.
 *
 * @see DKVShard
 * @see ShardedDKVClient
 */
public class DKVNodeSet {
    private final String name;
    private final DKVNode[] nodes;
    private final AtomicInteger idx = new AtomicInteger(0);

    public DKVNodeSet(String name, Set<DKVNode> nodes) {
        checkf(name != null && !name.trim().isEmpty(), IllegalArgumentException.class, "DKV node set name must be given");
        checkf(nodes != null && !nodes.isEmpty(), IllegalArgumentException.class, "DKV nodes must be given");

        //noinspection ConstantConditions
        this.nodes = nodes.toArray(new DKVNode[0]);
        this.name = name;
    }

    public String getName() {
        return name;
    }

    public DKVNode[] getNodes() {
        return nodes;
    }

    public DKVNode getNextNode() {
        if (nodes == null) throw new IllegalStateException("this DKVNodeSet instance is not initialized");
        int nextIdx = idx.getAndUpdate(id -> (id + 1) % nodes.length);
        return nodes[nextIdx];
    }

    // intended for deserialization
    @SuppressWarnings("unused")
    private DKVNodeSet() {
        this.name = null;
        this.nodes = null;
    }

    public int getNumNodes() {
        return nodes != null ? nodes.length : 0;
    }
}
