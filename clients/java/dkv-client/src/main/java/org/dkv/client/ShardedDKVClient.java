package org.dkv.client;

import com.google.common.collect.Iterables;
import dkv.serverpb.Api;

import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import static com.google.common.collect.Iterables.isEmpty;
import static com.google.common.collect.Iterables.size;
import static org.dkv.client.Utils.checkf;

public class ShardedDKVClient implements DKVClient {
    private final Map<DKVShard, SimpleDKVClient> dkvCliCache;
    private final ShardProvider shardProvider;

    public ShardedDKVClient(ShardProvider shardProvider) {
        checkf(shardProvider != null, IllegalArgumentException.class, "Shard provider must be provided");
        this.shardProvider = shardProvider;
        dkvCliCache = new ConcurrentHashMap<>();
    }

    @Override
    public void put(String key, String value) {
        Iterable<DKVShard> dkvShards = shardProvider.provideShards(DKVOpType.WRITE, key);
        checkf(!isEmpty(dkvShards), IllegalArgumentException.class, "unable to compute shard for the given key: %s", key);
        DKVClient dkvClient = getDKVClient(dkvShards);
        dkvClient.put(key, value);
    }

    @Override
    public void put(byte[] key, byte[] value) {
        Iterable<DKVShard> dkvShards = shardProvider.provideShards(DKVOpType.WRITE, key);
        checkf(!isEmpty(dkvShards), IllegalArgumentException.class, "unable to compute shard for the given key");
        DKVClient dkvClient = getDKVClient(dkvShards);
        dkvClient.put(key, value);
    }

    @Override
    public String get(Api.ReadConsistency consistency, String key) {
        Iterable<DKVShard> dkvShards = shardProvider.provideShards(DKVOpType.READ, key);
        checkf(!isEmpty(dkvShards), IllegalArgumentException.class, "unable to compute shard for the given key: %s", key);
        DKVClient dkvClient = getDKVClient(dkvShards);
        return dkvClient.get(consistency, key);
    }

    @Override
    public byte[] get(Api.ReadConsistency consistency, byte[] key) {
        Iterable<DKVShard> dkvShards = shardProvider.provideShards(DKVOpType.READ, key);
        checkf(!isEmpty(dkvShards), IllegalArgumentException.class, "unable to compute shard for the given key");
        DKVClient dkvClient = getDKVClient(dkvShards);
        return dkvClient.get(consistency, key);
    }

    @Override
    public String[] multiGet(Api.ReadConsistency consistency, String[] keys) {
        Iterable<DKVShard> dkvShards = shardProvider.provideShards(DKVOpType.READ, keys);
        checkf(!isEmpty(dkvShards), IllegalArgumentException.class, "unable to compute shard for the given keys");
        checkf(size(dkvShards) == 1, UnsupportedOperationException.class, "DKV does not support cross shard multi get");
        DKVClient dkvClient = getDKVClient(dkvShards);
        return dkvClient.multiGet(consistency, keys);
    }

    @Override
    public byte[][] multiGet(Api.ReadConsistency consistency, byte[][] keys) {
        Iterable<DKVShard> dkvShards = shardProvider.provideShards(DKVOpType.READ, keys);
        checkf(!isEmpty(dkvShards), IllegalArgumentException.class, "unable to compute shard for the given keys");
        checkf(size(dkvShards) == 1, UnsupportedOperationException.class, "DKV does not support cross shard multi get");
        DKVClient dkvClient = getDKVClient(dkvShards);
        return dkvClient.multiGet(consistency, keys);
    }

    @Override
    public Iterator<DKVEntry> iterate(String startKey) {
        Iterable<DKVShard> dkvShards = shardProvider.provideShards(DKVOpType.READ, startKey);
        checkf(!isEmpty(dkvShards), IllegalArgumentException.class, "unable to compute shard for the given start key: %s", startKey);
        DKVClient dkvClient = getDKVClient(dkvShards);
        return dkvClient.iterate(startKey);
    }

    @Override
    public Iterator<DKVEntry> iterate(byte[] startKey) {
        Iterable<DKVShard> dkvShards = shardProvider.provideShards(DKVOpType.READ, startKey);
        checkf(!isEmpty(dkvShards), IllegalArgumentException.class, "unable to compute shard for the given start key");
        DKVClient dkvClient = getDKVClient(dkvShards);
        return dkvClient.iterate(startKey);
    }

    @Override
    public Iterator<DKVEntry> iterate(String startKey, String keyPref) {
        Iterable<DKVShard> dkvShards = shardProvider.provideShards(DKVOpType.READ, startKey);
        checkf(!isEmpty(dkvShards), IllegalArgumentException.class, "unable to compute shard for the given start key: %s", startKey);
        DKVClient dkvClient = getDKVClient(dkvShards);
        return dkvClient.iterate(startKey, keyPref);
    }

    @Override
    public Iterator<DKVEntry> iterate(byte[] startKey, byte[] keyPref) {
        Iterable<DKVShard> dkvShards = shardProvider.provideShards(DKVOpType.READ, startKey);
        checkf(!isEmpty(dkvShards), IllegalArgumentException.class, "unable to compute shard for the given start key");
        DKVClient dkvClient = getDKVClient(dkvShards);
        return dkvClient.iterate(startKey, keyPref);
    }

    private DKVClient getDKVClient(Iterable<DKVShard> dkvShards) {
        DKVShard dkvShard = Iterables.get(dkvShards, 0);
        dkvCliCache.computeIfAbsent(dkvShard, shard -> new SimpleDKVClient(shard.getHost(), shard.getPort(), shard.getName()));
        return dkvCliCache.get(dkvShard);
    }

    @Override
    public void close() {
        for (SimpleDKVClient dkvClient : dkvCliCache.values()) {
            dkvClient.close();
        }
    }
}
