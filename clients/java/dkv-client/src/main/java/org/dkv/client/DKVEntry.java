package org.dkv.client;

import com.google.protobuf.ByteString;
import dkv.serverpb.Api;

public class DKVEntry {
    private final Api.Status iterStatus;
    private final ByteString iterKey;
    private final ByteString iterVal;

    public DKVEntry(Api.IterateResponse iterRes) {
        iterStatus = iterRes.getStatus();
        iterKey = iterRes.getKey();
        iterVal = iterRes.getValue();
    }

    public void checkStatus() {
        if (iterStatus.getCode() != 0) {
            throw new DKVException(iterStatus, "iterate", null);
        }
    }

    public String getKeyAsString() {
        return iterKey.toStringUtf8();
    }

    public byte[] getKeyAsByteArray() {
        return iterKey.toByteArray();
    }

    public String getValueAsString() {
        return iterVal.toStringUtf8();
    }

    public byte[] getValueAsByteArray() {
        return iterVal.toByteArray();
    }
}
