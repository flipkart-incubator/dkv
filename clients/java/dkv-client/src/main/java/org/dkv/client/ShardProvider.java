package org.dkv.client;

public interface ShardProvider {
    Iterable<DKVShard> provideShards(byte[]... keys);
    Iterable<DKVShard> provideShards(String... keys);
}
