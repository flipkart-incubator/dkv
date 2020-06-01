package org.dkv.client;

import dkv.serverpb.Api;

import java.util.Iterator;

public interface DKVClient {
    void put(String key, String value);

    void put(byte[] key, byte[] value);

    String get(Api.ReadConsistency consistency, String key);

    byte[] get(Api.ReadConsistency consistency, byte[] key);

    String[] multiGet(Api.ReadConsistency consistency, String[] keys);

    byte[][] multiGet(Api.ReadConsistency consistency, byte[][] keys);

    Iterator<DKVEntry> iterate(String startKey);

    Iterator<DKVEntry> iterate(byte[] startKey);

    Iterator<DKVEntry> iterate(String startKey, String keyPref);

    Iterator<DKVEntry> iterate(byte[] startKey, byte[] keyPref);
}
