package org.dkv.client;

import gnu.crypto.hash.RipeMD160;

import java.util.LinkedHashSet;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.dkv.client.Utils.checkf;

/**
 * An implementation of a shard provider that computes the
 * relevant {@link DKVShard} based on the hash value of the
 * respective key involved in the operation.
 *
 * <p>It uses the <a href="https://en.wikipedia.org/wiki/RIPEMD#RIPEMD-160_hashes">RipeMD160 algorithm</a> for computing the hash value
 * of the given key(s).
 */
public class KeyHashBasedShardProvider implements ShardProvider {
    private final ShardConfiguration shardConfiguration;

    public KeyHashBasedShardProvider(ShardConfiguration shardConfiguration) {
        checkf(shardConfiguration != null, IllegalArgumentException.class, "shard configuration must be provided");
        this.shardConfiguration = shardConfiguration;
    }

    @Override
    public Iterable<DKVShard> provideShards(byte[]... keys) {
        checkf(keys != null && keys.length > 0, IllegalArgumentException.class, "must provide at least one key for providing shards");
        LinkedHashSet<DKVShard> shards = new LinkedHashSet<>();
        //noinspection ConstantConditions
        for (byte[] key : keys) {
            int shardId = getShardId(key);
            shards.add(shardConfiguration.getShardAtIndex(shardId));
        }

        return shards;
    }

    @Override
    public Iterable<DKVShard> provideShards(String... keys) {
        checkf(keys != null && keys.length > 0, IllegalArgumentException.class, "must provide at least one key for providing shards");
        LinkedHashSet<DKVShard> shards = new LinkedHashSet<>();
        //noinspection ConstantConditions
        for (String key : keys) {
            int shardId = getShardId(key.getBytes(UTF_8));
            shards.add(shardConfiguration.getShardAtIndex(shardId));
        }
        return shards;
    }

    private int getShardId(byte[] key) {
        RipeMD160 hash = new RipeMD160();
        hash.update(key, 0, key.length);
        byte[] digest = hash.digest();
        int digestNum = 0;
        for (int i = 0; i < 4; i++) {
            digestNum |= (digest[i] & 0xFF) << (8 * i);
        }
        // digestNum can be negative, hence first AND turns it positive
        return (int) ((digestNum & 0xFFFF) % shardConfiguration.getNumShards());
    }
}
