package org.dkv.client;

import com.google.protobuf.ByteString;
import dkv.serverpb.Api;
import dkv.serverpb.DKVGrpc;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;

import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;

/**
 * An implementation of the {@link DKVClient} interface. It provides a convenient
 * wrapper around the underlying GRPC stubs for interacting with the DKV database.
 * All methods are invoked synchronously over the underlying GRPC channel and hence
 * are blocking. Users must wrap these calls around {@link DKVException} if they
 * wish to handle failures.
 *
 * This implementation has no non-final state and hence its instances are thread safe
 * for concurrent access.
 *
 * @see DKVClient
 * @see DKVException
 */
public class DKVClientImpl implements DKVClient {
    private final DKVGrpc.DKVBlockingStub blockingStub;

    /**
     * Creates an instance with the underlying GRPC conduit to the DKV database
     * running on the specified <tt>dkvHost</tt> and <tt>dkPort</tt>. Currently
     * all GRPC exchanges happen over an in-secure (non-TLS) based channel. Future
     * implementations will support additional options for securing these exchanges.
     *
     * @param dkvHost host on which DKV database is running
     * @param dkvPort port the DKV database is listening on
     * @throws IllegalArgumentException if the specified <tt>dkvHost</tt> or <tt>dkvPort</tt>
     * is invalid
     * @throws RuntimeException in case of any connection failures
     */
    public DKVClientImpl(String dkvHost, int dkvPort) {
        if (dkvHost == null || dkvHost.trim().length() == 0) {
            throw new IllegalArgumentException("Valid DKV hostname must be provided");
        }

        if (dkvPort <= 0) {
            throw new IllegalArgumentException("Valid DKV port must be provided");
        }

        ManagedChannelBuilder<?> channelBuilder = ManagedChannelBuilder.forAddress(dkvHost, dkvPort).usePlaintext();
        ManagedChannel channel = channelBuilder.build();
        blockingStub = DKVGrpc.newBlockingStub(channel);
    }

    public void put(String key, String value) {
        put(ByteString.copyFromUtf8(key), ByteString.copyFromUtf8(value));
    }

    public void put(byte[] key, byte[] value) {
        put(ByteString.copyFrom(key), ByteString.copyFrom(value));
    }

    public String get(Api.ReadConsistency consistency, String key) {
        ByteString value = get(consistency, ByteString.copyFromUtf8(key));
        return value.toStringUtf8();
    }

    public byte[] get(Api.ReadConsistency consistency, byte[] key) {
        ByteString value = get(consistency, ByteString.copyFrom(key));
        return value.toByteArray();
    }

    public String[] multiGet(Api.ReadConsistency consistency, String[] keys) {
        LinkedList<ByteString> keyByteStrs = new LinkedList<>();
        for (String key : keys) {
            keyByteStrs.add(ByteString.copyFromUtf8(key));
        }
        List<ByteString> valByteStrs = multiGet(consistency, keyByteStrs);
        String[] values = new String[valByteStrs.size()];
        int idx = 0;
        for (ByteString valByteStr : valByteStrs) {
            values[idx++] = valByteStr.toStringUtf8();
        }
        return values;
    }

    public byte[][] multiGet(Api.ReadConsistency consistency, byte[][] keys) {
        LinkedList<ByteString> keyByteStrs = new LinkedList<>();
        for (byte[] key : keys) {
            keyByteStrs.add(ByteString.copyFrom(key));
        }
        List<ByteString> valByteStrs = multiGet(consistency, keyByteStrs);
        byte[][] values = new byte[valByteStrs.size()][];
        int idx = 0;
        for (ByteString valByteStr : valByteStrs) {
            values[idx++] = valByteStr.toByteArray();
        }
        return values;
    }

    public Iterator<DKVEntry> iterate(String startKey) {
        Iterator<Api.IterateResponse> iterRes = iterate(
                ByteString.copyFromUtf8(startKey), ByteString.EMPTY);
        return new DKVEntryIterator(iterRes);
    }

    public Iterator<DKVEntry> iterate(byte[] startKey) {
        Iterator<Api.IterateResponse> iterRes = iterate(
                ByteString.copyFrom(startKey), ByteString.EMPTY);
        return new DKVEntryIterator(iterRes);
    }

    public Iterator<DKVEntry> iterate(String startKey, String keyPref) {
        Iterator<Api.IterateResponse> iterRes = iterate(
                ByteString.copyFromUtf8(startKey), ByteString.copyFromUtf8(keyPref));
        return new DKVEntryIterator(iterRes);
    }

    public Iterator<DKVEntry> iterate(byte[] startKey, byte[] keyPref) {
        Iterator<Api.IterateResponse> iterRes = iterate(
                ByteString.copyFrom(startKey), ByteString.copyFrom(keyPref));
        return new DKVEntryIterator(iterRes);
    }

    private Iterator<Api.IterateResponse> iterate(ByteString startKey, ByteString keyPref) {
        Api.IterateRequest.Builder iterReqBuilder = Api.IterateRequest.newBuilder();
        Api.IterateRequest iterReq = iterReqBuilder
                .setKeyPrefix(keyPref)
                .setStartKey(startKey)
                .build();
        return blockingStub.iterate(iterReq);
    }

    private void put(ByteString keyByteStr, ByteString valByteStr) {
        Api.PutRequest.Builder putReqBuilder = Api.PutRequest.newBuilder();
        Api.PutRequest putReq = putReqBuilder
                .setKey(keyByteStr)
                .setValue(valByteStr)
                .build();
        Api.Status status = blockingStub.put(putReq).getStatus();
        if (status.getCode() != 0) {
            throw new DKVException(status, "Put", new Object[]{keyByteStr.toByteArray(), valByteStr.toByteArray()});
        }
    }

    private ByteString get(Api.ReadConsistency consistency, ByteString keyByteStr) {
        Api.GetRequest.Builder getReqBuilder = Api.GetRequest.newBuilder();
        Api.GetRequest getReq = getReqBuilder
                .setKey(keyByteStr)
                .setReadConsistency(consistency)
                .build();
        Api.GetResponse getRes = blockingStub.get(getReq);
        Api.Status status = getRes.getStatus();
        if (status.getCode() != 0) {
            throw new DKVException(status, "Get", new Object[]{consistency, keyByteStr.toByteArray()});
        }
        return getRes.getValue();
    }

    private List<ByteString> multiGet(Api.ReadConsistency consistency, List<ByteString> keyByteStrs) {
        Api.MultiGetRequest.Builder multiGetReqBuilder = Api.MultiGetRequest.newBuilder();
        Api.MultiGetRequest multiGetReq = multiGetReqBuilder
                .addAllKeys(keyByteStrs)
                .setReadConsistency(consistency)
                .build();
        Api.MultiGetResponse multiGetRes = blockingStub.multiGet(multiGetReq);
        Api.Status status = multiGetRes.getStatus();
        if (status.getCode() != 0) {
            throw new DKVException(status, "MultiGet", new Object[]{consistency, keyByteStrs});
        }
        return multiGetRes.getValuesList();
    }
}
