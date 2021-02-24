package org.dkv.client;

import java.util.Map;
import java.util.Objects;

import static java.util.Collections.unmodifiableMap;
import static org.dkv.client.Utils.checkf;

/**
 * Represents a shard or partition of keyspace owned by
 * a DKV instance. It is identified by a user defined
 * name along with one or more {@link DKVNodeSet} instances
 * identified by the respective {@link DKVNodeType}.
 *
 * @see ShardedDKVClient
 */
public class DKVShard {
    private final String name;
    private final Map<DKVNodeType, DKVNodeSet> topology;

    public DKVShard(String name, Map<DKVNodeType, DKVNodeSet> topology) {
        checkf(name != null && !name.trim().isEmpty(), IllegalArgumentException.class, "shard name must be provided");
        validate(topology);

        this.name = name;
        this.topology = unmodifiableMap(topology);
    }

    public String getName() {
        return name;
    }

    public DKVNodeSet getNodesByType(DKVNodeType... nodeType) {
        checkf(this.topology != null, IllegalStateException.class, "topology is not initialized");
        for (DKVNodeType dkvNodeType : nodeType) {
            //noinspection ConstantConditions
            if (this.topology.containsKey(dkvNodeType)) {
                return this.topology.get(dkvNodeType);
            }
        }
        throw new IllegalArgumentException("valid DKV node type must be given");
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        DKVShard dkvShard = (DKVShard) o;
        return name.equals(dkvShard.name);
    }

    @Override
    public int hashCode() {
        return Objects.hash(name);
    }

    @Override
    public String toString() {
        return "DKVShard{" +
                "name='" + name + '\'' +
                ", topology=" + topology +
                '}';
    }

    private void validate(Map<DKVNodeType, DKVNodeSet> topology) {
        checkf(topology != null && !topology.isEmpty(), IllegalArgumentException.class, "topology must be given");
        //noinspection ConstantConditions
        for (Map.Entry<DKVNodeType, DKVNodeSet> topEntry : topology.entrySet()) {
            DKVNodeType nodeType = topEntry.getKey();
            DKVNodeSet nodes = topEntry.getValue();
            checkf(nodes != null, IllegalArgumentException.class, "DKV nodes must be given for node type: %s", nodeType.name());
        }
    }

    // intended for deserialization
    @SuppressWarnings("unused")
    private DKVShard() {
        this.name = null;
        this.topology = null;
    }
}
