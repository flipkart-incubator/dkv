package org.dkv.client;

import java.util.List;
import java.util.Map;

public interface ShardProvider {
    DKVShard provideShard(byte[] key);
    DKVShard provideShard(String key);

    Map<DKVShard, List<byte[]>> provideShards(byte[]... keys);
    Map<DKVShard, List<String>> provideShards(String... keys);
}
