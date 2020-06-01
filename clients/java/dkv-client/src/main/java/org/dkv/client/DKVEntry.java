package org.dkv.client;

import com.google.protobuf.ByteString;
import dkv.serverpb.Api;

/**
 * Represents a single entry retrieved from DKV database while
 * iterating through its keyspace using the <tt>iterate</tt> methods
 * of {@link DKVClient}.
 */
public class DKVEntry {
    private final Api.Status iterStatus;
    private final ByteString iterKey;
    private final ByteString iterVal;

    DKVEntry(Api.IterateResponse iterRes) {
        iterStatus = iterRes.getStatus();
        iterKey = iterRes.getKey();
        iterVal = iterRes.getValue();
    }

    /**
     * Checks if the current entry indicates an error retrieved
     * from the DKV database. Users must invoke this method before
     * reading any other state.
     *
     * @throws DKVException if the current entry has an error status
     */
    public void checkStatus() {
        if (iterStatus.getCode() != 0) {
            throw new DKVException(iterStatus, "iterate", null);
        }
    }

    /**
     * Retrieves the underlying key of this entry as a string.
     *
     * @return the key as <tt>String</tt>
     * @throws IllegalStateException if this entry is in error status
     */
    public String getKeyAsString() {
        if (iterKey == null) {
            throw new IllegalStateException("current entry is in error status");
        }
        return iterKey.toStringUtf8();
    }

    /**
     * Retrieves the underlying key of this entry as a byte array.
     *
     * @return the key as an array of bytes
     * @throws IllegalStateException if this entry is in error status
     */
    public byte[] getKeyAsByteArray() {
        if (iterKey == null) {
            throw new IllegalStateException("current entry is in error status");
        }
        return iterKey.toByteArray();
    }

    /**
     * Retrieves the underlying value of this entry as a string.
     *
     * @return the key as <tt>string</tt>
     * @throws IllegalStateException if this entry is in error status
     */
    public String getValueAsString() {
        if (iterKey == null) {
            throw new IllegalStateException("current entry is in error status");
        }
        return iterVal.toStringUtf8();
    }

    /**
     * Retrieves the underlying value of this entry as a byte array.
     *
     * @return the value as <tt>byte[]</tt>
     * @throws IllegalStateException if this entry is in error status
     */
    public byte[] getValueAsByteArray() {
        if (iterKey == null) {
            throw new IllegalStateException("current entry is in error status");
        }
        return iterVal.toByteArray();
    }
}
