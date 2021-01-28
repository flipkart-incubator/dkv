package org.dkv.client;

import com.google.protobuf.ByteString;
import dkv.serverpb.Api;
import dkv.serverpb.DKVGrpc;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;

import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;

import static com.google.protobuf.ByteString.*;

/**
 * An implementation of the {@link DKVClient} interface. It provides a convenient
 * wrapper around the underlying GRPC stubs for interacting with the DKV database.
 * All methods are invoked synchronously over the underlying GRPC channel and hence
 * are blocking. Users must wrap these calls around {@link DKVException} if they
 * wish to handle failures.
 *
 * <p>This implementation has no non-final state and hence its instances are thread safe
 * for concurrent access.
 *
 * @see DKVClient
 * @see DKVException
 */
public class SimpleDKVClient implements DKVClient {
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
    public SimpleDKVClient(String dkvHost, int dkvPort) {
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

    /**
     * Creates an instance with the underlying GRPC conduit to the DKV database
     * running on the specified <tt>dkvHost</tt> and <tt>dkPort</tt>. Currently
     * all GRPC exchanges happen over an in-secure (non-TLS) based channel. Future
     * implementations will support additional options for securing these exchanges.
     *
     * <p><tt>authority</tt> parameter can be used to send a user defined value inside
     * the HTTP/2 authority psuedo header as defined by
     * <a href="https://tools.ietf.org/html/rfc7540">RFC 7540</a>. A typical use case
     * for setting this parameter is the virtual host based routing to upstream DKV
     * clusters via an HTTP proxy such as NGINX or Envoy.
     *
     * @param dkvHost host on which DKV database is running
     * @param dkvPort port the DKV database is listening on
     * @param authority value to be sent inside the HTTP/2 authority header
     * @throws IllegalArgumentException if the specified <tt>dkvHost</tt> or <tt>dkvPort</tt>
     * is invalid
     * @throws RuntimeException in case of any connection failures
     */
    public SimpleDKVClient(String dkvHost, int dkvPort, String authority) {
        if (dkvHost == null || dkvHost.trim().length() == 0) {
            throw new IllegalArgumentException("Valid DKV hostname must be provided");
        }

        if (authority == null || authority.trim().length() == 0) {
            throw new IllegalArgumentException("Valid authority must be provided");
        }

        if (dkvPort <= 0) {
            throw new IllegalArgumentException("Valid DKV port must be provided");
        }

        ManagedChannelBuilder<?> channelBuilder = ManagedChannelBuilder.forAddress(dkvHost, dkvPort).usePlaintext().overrideAuthority(authority);
        ManagedChannel channel = channelBuilder.build();
        blockingStub = DKVGrpc.newBlockingStub(channel);
    }

    /**
     * Creates an instance with the underlying GRPC conduit to the DKV database
     * running on the specified <tt>dkvTarget</tt>. Currently
     * all GRPC exchanges happen over an in-secure (non-TLS) based channel. Future
     * implementations will support additional options for securing these exchanges.
     *
     * @param dkvTarget location (in the form host:port) at which DKV database is running
     * @throws IllegalArgumentException if the specified <tt>dkvHost</tt> or <tt>dkvPort</tt>
     * is invalid
     * @throws RuntimeException in case of any connection failures
     */
    public SimpleDKVClient(String dkvTarget) {
        if (dkvTarget == null || dkvTarget.trim().length() == 0) {
            throw new IllegalArgumentException("Valid DKV hostname must be provided");
        }

        ManagedChannelBuilder<?> channelBuilder = ManagedChannelBuilder.forTarget(dkvTarget).usePlaintext();
        ManagedChannel channel = channelBuilder.build();
        blockingStub = DKVGrpc.newBlockingStub(channel);
    }

    /**
     * Creates an instance with the underlying GRPC conduit to the DKV database
     * running on the specified <tt>dkvTarget</tt>. Currently
     * all GRPC exchanges happen over an in-secure (non-TLS) based channel. Future
     * implementations will support additional options for securing these exchanges.
     *
     * <p><tt>authority</tt> parameter can be used to send a user defined value inside
     * the HTTP/2 authority psuedo header as defined by
     * <a href="https://tools.ietf.org/html/rfc7540">RFC 7540</a>. A typical use case
     * for setting this parameter is the virtual host based routing to upstream DKV
     * clusters via an HTTP proxy such as NGINX or Envoy.
     *
     * @param dkvTarget location (in the form host:port) at which DKV database is running
     * @param authority value to be sent inside the HTTP/2 authority header
     * @throws IllegalArgumentException if the specified <tt>dkvHost</tt> or <tt>dkvPort</tt>
     * is invalid
     * @throws RuntimeException in case of any connection failures
     */
    public SimpleDKVClient(String dkvTarget, String authority) {
        if (dkvTarget == null || dkvTarget.trim().length() == 0) {
            throw new IllegalArgumentException("Valid DKV target (host:port) must be provided");
        }

        if (authority == null || authority.trim().length() == 0) {
            throw new IllegalArgumentException("Valid authority must be provided");
        }

        ManagedChannelBuilder<?> channelBuilder = ManagedChannelBuilder.forTarget(dkvTarget).usePlaintext().overrideAuthority(authority);
        ManagedChannel channel = channelBuilder.build();
        blockingStub = DKVGrpc.newBlockingStub(channel);
    }

    public void put(String key, String value) {
        put(copyFromUtf8(key), copyFromUtf8(value));
    }

    public void put(byte[] key, byte[] value) {
        put(copyFrom(key), copyFrom(value));
    }

    public String get(Api.ReadConsistency consistency, String key) {
        ByteString value = get(consistency, copyFromUtf8(key));
        return value.toStringUtf8();
    }

    public byte[] get(Api.ReadConsistency consistency, byte[] key) {
        ByteString value = get(consistency, copyFrom(key));
        return value.toByteArray();
    }

    public String[] multiGet(Api.ReadConsistency consistency, String[] keys) {
        LinkedList<ByteString> keyByteStrs = new LinkedList<>();
        for (String key : keys) {
            keyByteStrs.add(copyFromUtf8(key));
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
            keyByteStrs.add(copyFrom(key));
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
        return iterate(copyFromUtf8(startKey), EMPTY);
    }

    public Iterator<DKVEntry> iterate(byte[] startKey) {
        return iterate(copyFrom(startKey), EMPTY);
    }

    public Iterator<DKVEntry> iterate(String startKey, String keyPref) {
        return iterate(copyFromUtf8(startKey), copyFromUtf8(keyPref));
    }

    public Iterator<DKVEntry> iterate(byte[] startKey, byte[] keyPref) {
        return iterate(copyFrom(startKey), copyFrom(keyPref));
    }

    @Override
    public void close() {
        ((ManagedChannel) blockingStub.getChannel()).shutdownNow();
    }

    private Iterator<DKVEntry> iterate(ByteString startKey, ByteString keyPref) {
        Api.IterateRequest.Builder iterReqBuilder = Api.IterateRequest.newBuilder();
        Api.IterateRequest iterReq = iterReqBuilder
                .setKeyPrefix(keyPref)
                .setStartKey(startKey)
                .build();
        Iterator<Api.IterateResponse> iterRes = blockingStub.iterate(iterReq);
        return new DKVEntryIterator(iterRes);
    }

    private static class DKVEntryIterator implements Iterator<DKVEntry> {
        private final Iterator<Api.IterateResponse> iterRes;

        DKVEntryIterator(Iterator<Api.IterateResponse> iterRes) {
            this.iterRes = iterRes;
        }

        public boolean hasNext() {
            return iterRes.hasNext();
        }

        public DKVEntry next() {
            Api.IterateResponse iterateResponse = iterRes.next();
            return new DKVEntry(iterateResponse);
        }

        public void remove() {
            throw new UnsupportedOperationException();
        }
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
