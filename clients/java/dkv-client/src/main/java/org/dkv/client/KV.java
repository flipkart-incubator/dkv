package org.dkv.client;

public class KV<KeyType, ValueType> {
    private final KeyType key;
    private final ValueType value;
    private final long expiryTS;

    private KV(KeyType key, ValueType value) {
        this.key = key;
        this.value = value;
        this.expiryTS = 0;
    }

    private KV(KeyType key, ValueType value, long expiryTS) {
        this.key = key;
        this.value = value;
        this.expiryTS = expiryTS;
    }

    public final KeyType getKey() {
        return key;
    }

    public final ValueType getValue() {
        return value;
    }

    protected final long getExpiryTS(){
        return expiryTS;
    }

    public static final class Strings extends KV<String, String> {
        public Strings(String key, String value) {
            super(key, value);
        }

        public Strings(String key, String value, long expiryTS) {
            super(key, value, expiryTS);
        }

    }

    public static final class Bytes extends KV<byte[], byte[]> {
        public Bytes(byte[] key, byte[] value) {
            super(key, value);
        }

        public Bytes(byte[] key, byte[] value, long expiryTS) {
            super(key, value, expiryTS);
        }
    }
}
