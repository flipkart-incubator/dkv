package org.dkv.client;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
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
            DKVNode node = new DKVNode("host" + i, 1000 + i);
            ImmutableMap<DKVOpType, DKVNodeSet> topology = ImmutableMap.of(
                    DKVOpType.READ, new DKVNodeSet("slaves"+i, ImmutableSet.of(node)),
                    DKVOpType.WRITE, new DKVNodeSet("masters"+i, ImmutableSet.of(node))
            );
            dkvShards[i] = new DKVShard("shard" + i, topology);
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
        Iterable<DKVShard> dkvShards = shardProvider.provideShards(keys);
        assertEquals("Expected keys to distribute equally in all shards", NUM_SHARDS, size(dkvShards));
    }

    @Test(expected = IllegalArgumentException.class)
    public void shouldValidateEmptyKeys() {
        shardProvider.provideShards(new byte[][]{});
    }

    @Test(expected = IllegalArgumentException.class)
    public void shouldValidateInvalidShardConfiguration() {
        new KeyHashBasedShardProvider(null);
    }
}