package org.dkv.client;

import java.util.Set;

import static java.util.Collections.unmodifiableSet;
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
    private final Set<DKVNode> nodes;

    public DKVNodeSet(String name, Set<DKVNode> nodes) {
        checkf(name != null && !name.trim().isEmpty(), IllegalArgumentException.class, "DKV node set name must be given");
        checkf(nodes != null && !nodes.isEmpty(), IllegalArgumentException.class, "DKV nodes must be given");

        //noinspection ConstantConditions
        this.nodes = unmodifiableSet(nodes);
        this.name = name;
    }

    public String getName() {
        return name;
    }

    public Iterable<DKVNode> getNodes() {
        return nodes;
    }

    // intended for deserialization
    @SuppressWarnings("unused")
    private DKVNodeSet() {
        this.name = null;
        this.nodes = null;
    }
}
