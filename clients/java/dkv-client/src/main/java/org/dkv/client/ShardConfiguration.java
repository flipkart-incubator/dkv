package org.dkv.client;

import static org.dkv.client.Utils.checkf;

public class ShardConfiguration {
    private final DKVShard[] dkvShards;

    public ShardConfiguration(DKVShard[] dkvShards) {
        checkf(dkvShards != null && dkvShards.length > 0, IllegalArgumentException.class, "must provide DKV shards in shard configuration");
        //noinspection ConstantConditions
        this.dkvShards = new DKVShard[dkvShards.length];
        System.arraycopy(dkvShards, 0, this.dkvShards, 0, dkvShards.length);
    }

    public DKVShard getShardAtIndex(int idx) {
        checkf(idx >= 0 && idx < dkvShards.length, IllegalArgumentException.class, "given index is invalid: %d", idx);
        return dkvShards[idx];
    }

    public long getNumShards() {
        return dkvShards.length;
    }

    // intended for deserialization
    private ShardConfiguration() {
        this.dkvShards = null;
    }
}
