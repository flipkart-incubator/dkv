package org.dkv.client;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import org.junit.Before;
import org.junit.Test;

import java.util.List;
import java.util.Map;

import static java.lang.String.format;
import static org.junit.Assert.assertEquals;

public class KeyHashBasedShardProviderTest {

    private static final String KEY_PREFIX = "key_2561";
    private static final int NUM_SHARDS = 10;

    private ShardProvider shardProvider;

    @Before
    public void setup() {
        DKVShard[] dkvShards = new DKVShard[NUM_SHARDS];
        for (int i = 0; i < NUM_SHARDS; i++) {
            DKVNode node = new DKVNode("host" + i, 1000 + i);
            ImmutableMap<DKVNodeType, DKVNodeSet> topology = ImmutableMap.of(
                    DKVNodeType.SLAVE, new DKVNodeSet("slaves"+i, ImmutableSet.of(node)),
                    DKVNodeType.MASTER, new DKVNodeSet("masters"+i, ImmutableSet.of(node))
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
        Map<DKVShard, List<String>> dkvShards = shardProvider.provideShards(keys);
        assertEquals("Expected keys to distribute equally in all shards", NUM_SHARDS, dkvShards.size());
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