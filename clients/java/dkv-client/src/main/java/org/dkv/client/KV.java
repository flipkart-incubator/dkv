package org.dkv.client;

public class KV<KeyType, ValueType> {
    private final KeyType key;
    private final ValueType value;

    private KV(KeyType key, ValueType value) {
        this.key = key;
        this.value = value;
    }

    public final KeyType getKey() {
        return key;
    }

    public final ValueType getValue() {
        return value;
    }

    public static final class Strings extends KV<String, String> {
        public Strings(String key, String value) {
            super(key, value);
        }
    }

    public static final class Bytes extends KV<byte[], byte[]> {
        public Bytes(byte[] key, byte[] value) {
            super(key, value);
        }
    }
}
