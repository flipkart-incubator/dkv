package org.dkv.client;

public interface ShardProvider {
    Iterable<DKVShard> provideShards(DKVOpType opType, byte[]... keys);
    Iterable<DKVShard> provideShards(DKVOpType opType, String... keys);
}
