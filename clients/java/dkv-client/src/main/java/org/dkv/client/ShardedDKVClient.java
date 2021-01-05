package org.dkv.client;

import com.google.common.collect.Iterables;
import dkv.serverpb.Api;

import java.util.Iterator;

import static com.google.common.collect.Iterables.isEmpty;
import static com.google.common.collect.Iterables.size;
import static org.dkv.client.DKVNodeType.getNodeTypeByReadConsistency;
import static org.dkv.client.Utils.checkf;

/**
 * Implementation of a DKV client that can address multiple
 * DKV clusters each dedicated to a portion of the keyspace
 * called a shard. It depends on a concrete implementation
 * of a {@link ShardProvider} for resolving the respective
 * DKV shards involved in a given DKV operation.
 *
 * <p>Once the respective DKV shard is resolved, the implementation
 * creates an instance of {@link SimpleDKVClient} and invokes
 * the corresponding operation on it. Upon completion, the underlying
 * GRPC conduit is closed.
 *
 * @see SimpleDKVClient
 */
public class ShardedDKVClient implements DKVClient {
    private final ShardProvider shardProvider;

    public ShardedDKVClient(ShardProvider shardProvider) {
        checkf(shardProvider != null, IllegalArgumentException.class, "Shard provider must be provided");
        this.shardProvider = shardProvider;
    }

    @Override
    public void put(String key, String value) {
        Iterable<DKVShard> dkvShards = shardProvider.provideShards(key);
        checkf(!isEmpty(dkvShards), IllegalArgumentException.class, "unable to compute shard for the given key: %s", key);
        try (DKVClient dkvClient = getDKVClient(dkvShards, DKVNodeType.MASTER)) {
            dkvClient.put(key, value);
        }
    }

    @Override
    public void put(byte[] key, byte[] value) {
        Iterable<DKVShard> dkvShards = shardProvider.provideShards(key);
        checkf(!isEmpty(dkvShards), IllegalArgumentException.class, "unable to compute shard for the given key");
        try (DKVClient dkvClient = getDKVClient(dkvShards, DKVNodeType.MASTER)) {
            dkvClient.put(key, value);
        }
    }

    @Override
    public String get(Api.ReadConsistency consistency, String key) {
        Iterable<DKVShard> dkvShards = shardProvider.provideShards(key);
        checkf(!isEmpty(dkvShards), IllegalArgumentException.class, "unable to compute shard for the given key: %s", key);
        DKVNodeType nodeType = getNodeTypeByReadConsistency(consistency);
        try (DKVClient dkvClient = getDKVClient(dkvShards, nodeType)) {
            return dkvClient.get(consistency, key);
        }
    }

    @Override
    public byte[] get(Api.ReadConsistency consistency, byte[] key) {
        Iterable<DKVShard> dkvShards = shardProvider.provideShards(key);
        checkf(!isEmpty(dkvShards), IllegalArgumentException.class, "unable to compute shard for the given key");
        DKVNodeType nodeType = getNodeTypeByReadConsistency(consistency);
        try (DKVClient dkvClient = getDKVClient(dkvShards, nodeType)) {
            return dkvClient.get(consistency, key);
        }
    }

    @Override
    public String[] multiGet(Api.ReadConsistency consistency, String[] keys) {
        Iterable<DKVShard> dkvShards = shardProvider.provideShards(keys);
        checkf(!isEmpty(dkvShards), IllegalArgumentException.class, "unable to compute shard for the given keys");
        checkf(size(dkvShards) == 1, UnsupportedOperationException.class, "DKV does not support cross shard multi get");
        DKVNodeType nodeType = getNodeTypeByReadConsistency(consistency);
        try (DKVClient dkvClient = getDKVClient(dkvShards, nodeType)) {
            return dkvClient.multiGet(consistency, keys);
        }
    }

    @Override
    public byte[][] multiGet(Api.ReadConsistency consistency, byte[][] keys) {
        Iterable<DKVShard> dkvShards = shardProvider.provideShards(keys);
        checkf(!isEmpty(dkvShards), IllegalArgumentException.class, "unable to compute shard for the given keys");
        checkf(size(dkvShards) == 1, UnsupportedOperationException.class, "DKV does not support cross shard multi get");
        DKVNodeType nodeType = getNodeTypeByReadConsistency(consistency);
        try (DKVClient dkvClient = getDKVClient(dkvShards, nodeType)) {
            return dkvClient.multiGet(consistency, keys);
        }
    }

    @Override
    public Iterator<DKVEntry> iterate(String startKey) {
        Iterable<DKVShard> dkvShards = shardProvider.provideShards(startKey);
        checkf(!isEmpty(dkvShards), IllegalArgumentException.class, "unable to compute shard for the given start key: %s", startKey);
        try (DKVClient dkvClient = getDKVClient(dkvShards, DKVNodeType.SLAVE)) {
            return dkvClient.iterate(startKey);
        }
    }

    @Override
    public Iterator<DKVEntry> iterate(byte[] startKey) {
        Iterable<DKVShard> dkvShards = shardProvider.provideShards(startKey);
        checkf(!isEmpty(dkvShards), IllegalArgumentException.class, "unable to compute shard for the given start key");
        try (DKVClient dkvClient = getDKVClient(dkvShards, DKVNodeType.SLAVE)) {
            return dkvClient.iterate(startKey);
        }
    }

    @Override
    public Iterator<DKVEntry> iterate(String startKey, String keyPref) {
        Iterable<DKVShard> dkvShards = shardProvider.provideShards(startKey);
        checkf(!isEmpty(dkvShards), IllegalArgumentException.class, "unable to compute shard for the given start key: %s", startKey);
        try (DKVClient dkvClient = getDKVClient(dkvShards, DKVNodeType.SLAVE)) {
            return dkvClient.iterate(startKey, keyPref);
        }
    }

    @Override
    public Iterator<DKVEntry> iterate(byte[] startKey, byte[] keyPref) {
        Iterable<DKVShard> dkvShards = shardProvider.provideShards(startKey);
        checkf(!isEmpty(dkvShards), IllegalArgumentException.class, "unable to compute shard for the given start key");
        try (DKVClient dkvClient = getDKVClient(dkvShards, DKVNodeType.SLAVE)) {
            return dkvClient.iterate(startKey, keyPref);
        }
    }

    private DKVClient getDKVClient(Iterable<DKVShard> dkvShards, DKVNodeType nodeType) {
        DKVShard dkvShard = Iterables.get(dkvShards, 0);
        DKVNodeSet nodeSet = dkvShard.getNodesByType(nodeType);
        DKVNode dkvNode = Iterables.get(nodeSet.getNodes(), 0);
        return new SimpleDKVClient(dkvNode.getHost(), dkvNode.getPort(), nodeSet.getName());
    }

    @Override
    public void close() {
        // no-op
    }
}
