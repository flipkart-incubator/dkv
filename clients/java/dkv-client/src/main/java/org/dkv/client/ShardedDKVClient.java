package org.dkv.client;

import com.google.common.collect.Iterables;
import dkv.serverpb.Api;

import java.util.HashMap;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Map;

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
    private final ShardProvider shardProvider;

    public ShardedDKVClient(ShardProvider shardProvider) {
        checkf(shardProvider != null, IllegalArgumentException.class, "Shard provider must be provided");
        this.shardProvider = shardProvider;
    }

    @Override
    public void put(String key, String value) {
        DKVShard dkvShard = shardProvider.provideShard(key);
        checkf(dkvShard != null, IllegalArgumentException.class, "unable to compute shard for the given key: %s", key);
        //noinspection ConstantConditions
        try (DKVClient dkvClient = getDKVClient(dkvShard, MASTER, UNKNOWN)) {
            dkvClient.put(key, value);
        }
    }

    @Override
    public void put(byte[] key, byte[] value) {
        DKVShard dkvShard = shardProvider.provideShard(key);
        checkf(dkvShard != null, IllegalArgumentException.class, "unable to compute shard for the given key");
        //noinspection ConstantConditions
        try (DKVClient dkvClient = getDKVClient(dkvShard, MASTER, UNKNOWN)) {
            dkvClient.put(key, value);
        }
    }

    @Override
    public String get(Api.ReadConsistency consistency, String key) {
        DKVShard dkvShard = shardProvider.provideShard(key);
        checkf(dkvShard != null, IllegalArgumentException.class, "unable to compute shard for the given key: %s", key);
        DKVNodeType nodeType = getNodeTypeByReadConsistency(consistency);
        //noinspection ConstantConditions
        try (DKVClient dkvClient = getDKVClient(dkvShard, nodeType, UNKNOWN)) {
            return dkvClient.get(consistency, key);
        }
    }

    @Override
    public byte[] get(Api.ReadConsistency consistency, byte[] key) {
        DKVShard dkvShard = shardProvider.provideShard(key);
        checkf(dkvShard != null, IllegalArgumentException.class, "unable to compute shard for the given key");
        DKVNodeType nodeType = getNodeTypeByReadConsistency(consistency);
        //noinspection ConstantConditions
        try (DKVClient dkvClient = getDKVClient(dkvShard, nodeType, UNKNOWN)) {
            return dkvClient.get(consistency, key);
        }
    }

    @Override
    public String[] multiGet(Api.ReadConsistency consistency, String[] keys) {
        checkf(keys != null && keys.length > 0, IllegalArgumentException.class, "must provide at least one key for multi get");
        Map<DKVShard, List<String>> dkvShards = shardProvider.provideShards(keys);
        checkf(dkvShards != null && !dkvShards.isEmpty(), IllegalArgumentException.class, "unable to compute shard(s) for the given keys");
        DKVNodeType nodeType = getNodeTypeByReadConsistency(consistency);
        //noinspection ConstantConditions
        if (dkvShards.size() > 1) {
            checkf(consistency != Api.ReadConsistency.LINEARIZABLE, UnsupportedOperationException.class,
                    "DKV does not yet support cross shard linearizable multi get");

            //noinspection ConstantConditions
            HashMap<String, String> tempResult = new HashMap<>(keys.length);
            for (Map.Entry<DKVShard, List<String>> entry : dkvShards.entrySet()) {
                DKVShard dkvShard = entry.getKey();
                try (DKVClient dkvClient = getDKVClient(dkvShard, nodeType, UNKNOWN)) {
                    String[] reqKeys = entry.getValue().toArray(new String[0]);
                    String[] reqVals = dkvClient.multiGet(consistency, reqKeys);
                    for (int i = 0, reqKeysLength = reqKeys.length; i < reqKeysLength; i++) {
                        tempResult.put(reqKeys[i], reqVals[i]);
                    }
                }
            }
            String[] resVals = new String[keys.length];
            for (int i = 0, keysLength = keys.length; i < keysLength; i++) {
                resVals[i] = tempResult.get(keys[i]);
            }
            return resVals;
        } else {
            try (DKVClient dkvClient = getDKVClient(Iterables.get(dkvShards.keySet(), 0), nodeType, UNKNOWN)) {
                return dkvClient.multiGet(consistency, keys);
            }
        }
    }

    @Override
    public byte[][] multiGet(Api.ReadConsistency consistency, byte[][] keys) {
        checkf(keys != null && keys.length > 0, IllegalArgumentException.class, "must provide at least one key for multi get");
        Map<DKVShard, List<byte[]>> dkvShards = shardProvider.provideShards(keys);
        checkf(dkvShards != null && !dkvShards.isEmpty(), IllegalArgumentException.class, "unable to compute shard(s) for the given keys");
        DKVNodeType nodeType = getNodeTypeByReadConsistency(consistency);
        //noinspection ConstantConditions
        if (dkvShards.size() > 1) {
            checkf(consistency != Api.ReadConsistency.LINEARIZABLE, UnsupportedOperationException.class,
                    "DKV does not yet support cross shard linearizable multi get");

            //noinspection ConstantConditions
            IdentityHashMap<byte[], byte[]> tempResult = new IdentityHashMap<>(keys.length);
            for (Map.Entry<DKVShard, List<byte[]>> entry : dkvShards.entrySet()) {
                DKVShard dkvShard = entry.getKey();
                try (DKVClient dkvClient = getDKVClient(dkvShard, nodeType, UNKNOWN)) {
                    byte[][] reqKeys = entry.getValue().toArray(new byte[0][0]);
                    byte[][] reqVals = dkvClient.multiGet(consistency, reqKeys);
                    for (int i = 0, reqKeysLength = reqKeys.length; i < reqKeysLength; i++) {
                        tempResult.put(reqKeys[i], reqVals[i]);
                    }
                }
            }
            byte[][] resVals = new byte[keys.length][];
            for (int i = 0, keysLength = keys.length; i < keysLength; i++) {
                resVals[i] = tempResult.get(keys[i]);
            }
            return resVals;
        } else {
            try (DKVClient dkvClient = getDKVClient(Iterables.get(dkvShards.keySet(), 0), nodeType, UNKNOWN)) {
                return dkvClient.multiGet(consistency, keys);
            }
        }
    }

    @Override
    public DKVEntryIterator iterate(String startKey) {
        DKVShard dkvShard = shardProvider.provideShard(startKey);
        checkf(dkvShard != null, IllegalArgumentException.class, "unable to compute shard for the given start key: %s", startKey);
        //noinspection ConstantConditions
        DKVClient dkvClient = getDKVClient(dkvShard, SLAVE, UNKNOWN);
        return dkvClient.iterate(startKey);
    }

    @Override
    public DKVEntryIterator iterate(byte[] startKey) {
        DKVShard dkvShard = shardProvider.provideShard(startKey);
        checkf(dkvShard != null, IllegalArgumentException.class, "unable to compute shard for the given start key");
        //noinspection ConstantConditions
        DKVClient dkvClient = getDKVClient(dkvShard, SLAVE, UNKNOWN);
        return dkvClient.iterate(startKey);
    }

    @Override
    public DKVEntryIterator iterate(String startKey, String keyPref) {
        DKVShard dkvShard = shardProvider.provideShard(startKey);
        checkf(dkvShard != null, IllegalArgumentException.class, "unable to compute shard for the given start key: %s", startKey);
        //noinspection ConstantConditions
        DKVClient dkvClient = getDKVClient(dkvShard, SLAVE, UNKNOWN);
        return dkvClient.iterate(startKey, keyPref);
    }

    @Override
    public DKVEntryIterator iterate(byte[] startKey, byte[] keyPref) {
        DKVShard dkvShard = shardProvider.provideShard(startKey);
        checkf(dkvShard != null, IllegalArgumentException.class, "unable to compute shard for the given start key");
        //noinspection ConstantConditions
        DKVClient dkvClient = getDKVClient(dkvShard, SLAVE, UNKNOWN);
        return dkvClient.iterate(startKey, keyPref);
    }

    private DKVClient getDKVClient(DKVShard dkvShard, DKVNodeType... nodeType) {
        DKVNodeSet nodeSet = dkvShard.getNodesByType(nodeType);
        DKVNode dkvNode = Iterables.get(nodeSet.getNodes(), 0);
        return new SimpleDKVClient(dkvNode.getHost(), dkvNode.getPort(), nodeSet.getName());
    }

    @Override
    public void close() {
        // no-op
    }
}
