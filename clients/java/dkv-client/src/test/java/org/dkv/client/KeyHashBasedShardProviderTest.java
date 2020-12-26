package org.dkv.client;

import org.junit.Before;
import org.junit.Test;

import static com.google.common.collect.Iterables.size;
import static java.lang.String.format;
import static org.junit.Assert.assertEquals;

public class KeyHashBasedShardProviderTest {

    private static final String KEY_PREFIX = "key_851";
    private static final int NUM_SHARDS = 10;

    private ShardProvider shardProvider;

    @Before
    public void setup() {
        DKVShard[] dkvShards = new DKVShard[NUM_SHARDS];
        for (int i = 0; i < NUM_SHARDS; i++) {
            dkvShards[i] = new DKVShard("shard" + i, "host" + i, 1000 + i);
        }
        ShardConfiguration shardConfiguration = new ShardConfiguration(dkvShards);
        shardProvider = new KeyHashBasedShardProvider(shardConfiguration);
    }

    @Test
    public void shouldComputeShardsBasedOnKeys() {
        String[] keys = new String[NUM_SHARDS];
        for (int i = 0; i < NUM_SHARDS; i++) {
            keys[i] = format("%s%d", KEY_PREFIX, i);
        }
        Iterable<DKVShard> dkvShards = shardProvider.provideShards(DKVOpType.UNKNOWN, keys);
        assertEquals("Expected keys to distribute equally in all shards", NUM_SHARDS, size(dkvShards));
    }

    @Test(expected = IllegalArgumentException.class)
    public void shouldValidateEmptyKeys() {
        shardProvider.provideShards(DKVOpType.UNKNOWN, new byte[][]{});
    }

    @Test(expected = IllegalArgumentException.class)
    public void shouldValidateInvalidShardConfiguration() {
        new KeyHashBasedShardProvider(null);
    }
}