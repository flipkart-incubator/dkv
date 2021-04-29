package org.dkv.client;

import com.github.benmanes.caffeine.cache.*;
import com.google.common.collect.Iterables;
import dkv.serverpb.Api;

import java.io.Closeable;
import java.util.*;
import java.util.function.BinaryOperator;
import java.util.function.UnaryOperator;

import static java.util.Collections.addAll;
import static org.dkv.client.DKVNodeType.*;
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
 * @see DKVShard
 * @see ShardProvider
 * @see SimpleDKVClient
 */
public class ShardedDKVClient implements DKVClient {
    private static final int POOL_SIZE = 1000;
    private final ShardProvider shardProvider;
    private final DKVClientPool pool;

    public ShardedDKVClient(ShardProvider shardProvider) {
        checkf(shardProvider != null, IllegalArgumentException.class, "Shard provider must be provided");
        this.shardProvider = shardProvider;
        this.pool = new DKVClientPool(POOL_SIZE);
    }

    @Override
    public void put(String key, String value) {
        DKVShard dkvShard = shardProvider.provideShard(key);
        checkf(dkvShard != null, IllegalArgumentException.class, "unable to compute shard for the given key: %s", key);
        //noinspection ConstantConditions
        DKVClient dkvClient = pool.getDKVClient(dkvShard, MASTER, UNKNOWN);
        dkvClient.put(key, value);
    }

    @Override
    public void put(byte[] key, byte[] value) {
        DKVShard dkvShard = shardProvider.provideShard(key);
        checkf(dkvShard != null, IllegalArgumentException.class, "unable to compute shard for the given key");
        //noinspection ConstantConditions
        DKVClient dkvClient = pool.getDKVClient(dkvShard, MASTER, UNKNOWN);
        dkvClient.put(key, value);
    }

    @Override
    public boolean compareAndSet(byte[] key, byte[] expect, byte[] update) {
        return false;
    }

    @Override
    public long incrementAndGet(byte[] key) {
        return 0;
    }

    @Override
    public <T extends Number> T decrementAndGet(byte[] key) {
        return null;
    }

    @Override
    public <T extends Number> T addAndGet(byte[] key, T delta) {
        return null;
    }

    @Override
    public <T extends Number> T accumulateAndGet(byte[] key, BinaryOperator<T> operator) {
        return null;
    }

    @Override
    public <T extends Number> T updateAndGet(byte[] key, UnaryOperator<T> operator) {
        return null;
    }

    @Override
    public String get(Api.ReadConsistency consistency, String key) {
        DKVShard dkvShard = shardProvider.provideShard(key);
        checkf(dkvShard != null, IllegalArgumentException.class, "unable to compute shard for the given key: %s", key);
        DKVNodeType nodeType = getNodeTypeByReadConsistency(consistency);
        //noinspection ConstantConditions
        DKVClient dkvClient = pool.getDKVClient(dkvShard, nodeType, UNKNOWN);
        return dkvClient.get(consistency, key);
    }

    @Override
    public byte[] get(Api.ReadConsistency consistency, byte[] key) {
        DKVShard dkvShard = shardProvider.provideShard(key);
        checkf(dkvShard != null, IllegalArgumentException.class, "unable to compute shard for the given key");
        DKVNodeType nodeType = getNodeTypeByReadConsistency(consistency);
        //noinspection ConstantConditions
        DKVClient dkvClient = pool.getDKVClient(dkvShard, nodeType, UNKNOWN);
        return dkvClient.get(consistency, key);
    }

    @Override
    public KV.Strings[] multiGet(Api.ReadConsistency consistency, String[] keys) {
        checkf(keys != null && keys.length > 0, IllegalArgumentException.class, "must provide at least one key for multi get");
        Map<DKVShard, List<String>> dkvShards = shardProvider.provideShards(keys);
        checkf(dkvShards != null && !dkvShards.isEmpty(), IllegalArgumentException.class, "unable to compute shard(s) for the given keys");
        DKVNodeType nodeType = getNodeTypeByReadConsistency(consistency);
        //noinspection ConstantConditions
        if (dkvShards.size() > 1) {
            checkf(consistency != Api.ReadConsistency.LINEARIZABLE, UnsupportedOperationException.class,
                    "DKV does not yet support cross shard linearizable multi get");

            LinkedList<KV.Strings> result = new LinkedList<>();
            for (Map.Entry<DKVShard, List<String>> entry : dkvShards.entrySet()) {
                DKVClient dkvClient = pool.getDKVClient(entry.getKey(), nodeType, UNKNOWN);
                String[] reqKeys = entry.getValue().toArray(new String[0]);
                KV.Strings[] kvs = dkvClient.multiGet(consistency, reqKeys);
                addAll(result, kvs);
            }
            return result.toArray(new KV.Strings[0]);
        } else {
            DKVShard dkvShard = Iterables.get(dkvShards.keySet(), 0);
            DKVClient dkvClient = pool.getDKVClient(dkvShard, nodeType, UNKNOWN);
            return dkvClient.multiGet(consistency, keys);
        }
    }

    @Override
    public KV.Bytes[] multiGet(Api.ReadConsistency consistency, byte[][] keys) {
        checkf(keys != null && keys.length > 0, IllegalArgumentException.class, "must provide at least one key for multi get");
        Map<DKVShard, List<byte[]>> dkvShards = shardProvider.provideShards(keys);
        checkf(dkvShards != null && !dkvShards.isEmpty(), IllegalArgumentException.class, "unable to compute shard(s) for the given keys");
        DKVNodeType nodeType = getNodeTypeByReadConsistency(consistency);
        //noinspection ConstantConditions
        if (dkvShards.size() > 1) {
            checkf(consistency != Api.ReadConsistency.LINEARIZABLE, UnsupportedOperationException.class,
                    "DKV does not yet support cross shard linearizable multi get");

            LinkedList<KV.Bytes> result = new LinkedList<>();
            for (Map.Entry<DKVShard, List<byte[]>> entry : dkvShards.entrySet()) {
                DKVClient dkvClient = pool.getDKVClient(entry.getKey(), nodeType, UNKNOWN);
                byte[][] reqKeys = entry.getValue().toArray(new byte[0][]);
                KV.Bytes[] kvs = dkvClient.multiGet(consistency, reqKeys);
                addAll(result, kvs);
            }
            return result.toArray(new KV.Bytes[0]);
        } else {
            DKVShard dkvShard = Iterables.get(dkvShards.keySet(), 0);
            DKVClient dkvClient = pool.getDKVClient(dkvShard, nodeType, UNKNOWN);
            return dkvClient.multiGet(consistency, keys);
        }
    }

    @Override
    public Iterator<DKVEntry> iterate(String startKey) {
        DKVShard dkvShard = shardProvider.provideShard(startKey);
        checkf(dkvShard != null, IllegalArgumentException.class, "unable to compute shard for the given start key: %s", startKey);
        //noinspection ConstantConditions
        DKVClient dkvClient = pool.getDKVClient(dkvShard, SLAVE, UNKNOWN);
        return dkvClient.iterate(startKey);
    }

    @Override
    public Iterator<DKVEntry> iterate(byte[] startKey) {
        DKVShard dkvShard = shardProvider.provideShard(startKey);
        checkf(dkvShard != null, IllegalArgumentException.class, "unable to compute shard for the given start key");
        //noinspection ConstantConditions
        DKVClient dkvClient = pool.getDKVClient(dkvShard, SLAVE, UNKNOWN);
        return dkvClient.iterate(startKey);
    }

    @Override
    public Iterator<DKVEntry> iterate(String startKey, String keyPref) {
        DKVShard dkvShard = shardProvider.provideShard(startKey);
        checkf(dkvShard != null, IllegalArgumentException.class, "unable to compute shard for the given start key: %s", startKey);
        //noinspection ConstantConditions
        DKVClient dkvClient = pool.getDKVClient(dkvShard, SLAVE, UNKNOWN);
        return dkvClient.iterate(startKey, keyPref);
    }

    @Override
    public Iterator<DKVEntry> iterate(byte[] startKey, byte[] keyPref) {
        DKVShard dkvShard = shardProvider.provideShard(startKey);
        checkf(dkvShard != null, IllegalArgumentException.class, "unable to compute shard for the given start key");
        //noinspection ConstantConditions
        DKVClient dkvClient = pool.getDKVClient(dkvShard, SLAVE, UNKNOWN);
        return dkvClient.iterate(startKey, keyPref);
    }

    @Override
    public void close() {
        pool.close();
    }

    private static class DKVClientPool implements Closeable,
            RemovalListener<DKVClientPool.Key, SimpleDKVClient>, CacheLoader<DKVClientPool.Key, SimpleDKVClient> {

        private static class Key {
            private final DKVNode dkvNode;
            private final String authority;

            private Key(DKVNode dkvNode, String authority) {
                this.dkvNode = dkvNode;
                this.authority = authority;
            }

            @Override
            public boolean equals(Object o) {
                if (this == o) return true;
                if (o == null || getClass() != o.getClass()) return false;
                Key that = (Key) o;
                return Objects.equals(dkvNode, that.dkvNode) && Objects.equals(authority, that.authority);
            }

            @Override
            public int hashCode() {
                return Objects.hash(dkvNode, authority);
            }
        }

        private final LoadingCache<Key, SimpleDKVClient> internalPool;

        private DKVClientPool(long poolSize) {
            internalPool = Caffeine.newBuilder().maximumSize(poolSize).removalListener(this).build(this);
        }

        SimpleDKVClient getDKVClient(DKVShard dkvShard, DKVNodeType... nodeTypes) {
            DKVNodeSet nodeSet = dkvShard.getNodesByType(nodeTypes);
            DKVNode dkvNode = Iterables.get(nodeSet.getNodes(), 0);
            return internalPool.get(new Key(dkvNode, nodeSet.getName()));
        }

        @Override
        public void close() {
            internalPool.invalidateAll();
        }

        @Override
        public void onRemoval(Key id, SimpleDKVClient client, RemovalCause removalCause) {
            if (client != null) {
                client.close();
            }
        }

        @Override
        public SimpleDKVClient load(ShardedDKVClient.DKVClientPool.Key key) {
            return new SimpleDKVClient(key.dkvNode.getHost(), key.dkvNode.getPort(), key.authority);
        }

        @Override
        public SimpleDKVClient reload(ShardedDKVClient.DKVClientPool.Key key, SimpleDKVClient oldClient) {
            oldClient.close();
            return load(key);
        }
    }
}
