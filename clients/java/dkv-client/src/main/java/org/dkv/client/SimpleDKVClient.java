package org.dkv.client;

import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.jmx.JmxReporter;
import com.google.protobuf.ByteString;
import dkv.serverpb.Api;
import dkv.serverpb.DKVGrpc;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import org.dkv.client.metrics.MetricsInterceptor;

import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.TimeUnit;

import static com.google.protobuf.ByteString.*;
import static org.dkv.client.Utils.*;

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
    private static final String DEF_METRIC_PREFIX = "dkv-client-java";
    private static final MetricRegistry metrics = new MetricRegistry();

    private final DKVGrpc.DKVBlockingStub blockingStub;
    private final ManagedChannel channel;
    private final JmxReporter reporter;

    /**
     * Creates an instance with the underlying GRPC conduit to the DKV database
     * running on the specified <tt>dkvHost</tt> and <tt>dkPort</tt>. Currently
     * all GRPC exchanges happen over an in-secure (non-TLS) based channel. Future
     * implementations will support additional options for securing these exchanges.
     *
     * @param dkvHost host on which DKV database is running
     * @param dkvPort port the DKV database is listening on
     * @param metricPrefix prefix for the published metrics
     * @throws IllegalArgumentException if the specified <tt>dkvHost</tt> or <tt>dkvPort</tt>
     * is invalid
     * @throws RuntimeException in case of any connection failures
     */
    public SimpleDKVClient(String dkvHost, int dkvPort, String metricPrefix) {
        this(getManagedChannelBuilder(dkvHost, dkvPort), metricPrefix);
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
     * @param metricPrefix prefix for the published metrics
     * @throws IllegalArgumentException if the specified <tt>dkvHost</tt> or <tt>dkvPort</tt>
     * is invalid
     * @throws RuntimeException in case of any connection failures
     */
    public SimpleDKVClient(String dkvHost, int dkvPort, String authority, String metricPrefix) {
        this(getManagedChannelBuilder(dkvHost, dkvPort, authority), metricPrefix);
    }

    /**
     * Creates an instance with the underlying GRPC conduit to the DKV database
     * running on the specified <tt>dkvTarget</tt>. Currently
     * all GRPC exchanges happen over an in-secure (non-TLS) based channel. Future
     * implementations will support additional options for securing these exchanges.
     *
     * @param dkvTarget location (in the form host:port) at which DKV database is running
     * @param metricPrefix prefix for the published metrics
     * @throws IllegalArgumentException if the specified <tt>dkvHost</tt> or <tt>dkvPort</tt>
     * is invalid
     * @throws RuntimeException in case of any connection failures
     */
    public SimpleDKVClient(String dkvTarget, String metricPrefix) {
        this(getManagedChannelBuilder(dkvTarget), metricPrefix);
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
     * @param metricPrefix prefix for the published metrics
     * @throws IllegalArgumentException if the specified <tt>dkvHost</tt> or <tt>dkvPort</tt>
     * is invalid
     * @throws RuntimeException in case of any connection failures
     */
    public SimpleDKVClient(String dkvTarget, String authority, String metricPrefix) {
        this(getManagedChannelBuilder(dkvTarget, authority), metricPrefix);
    }

    @Override
    public void put(String key, String value) {
        put(copyFromUtf8(key), copyFromUtf8(value), 0L);
    }

    @Override
    public void put(byte[] key, byte[] value) {
        put(copyFrom(key), copyFrom(value), 0L);
    }

    @Override
    public void put(String key, String value, long expiryTS) {
        put(copyFromUtf8(key), copyFromUtf8(value), expiryTS);
    }

    @Override
    public void put(byte[] key, byte[] value, long expiryTS) {
        put(copyFrom(key), copyFrom(value), expiryTS);
    }

    @Override
    public boolean compareAndSet(byte[] key, byte[] expect, byte[] update) {
        ByteString expectByteStr = expect != null ? copyFrom(expect) : EMPTY;
        return cas(copyFrom(key), expectByteStr, copyFrom(update));
    }

    @Override
    public long incrementAndGet(byte[] key) {
        return addAndGet(key, 1);
    }

    @Override
    public long decrementAndGet(byte[] key) {
        return addAndGet(key, -1);
    }

    @Override
    public long addAndGet(byte[] key, long delta) {
        ByteString keyByteStr = copyFrom(key);
        return addAndGet(keyByteStr, delta);
    }

    @Override
    public String get(Api.ReadConsistency consistency, String key) {
        ByteString value = get(consistency, copyFromUtf8(key));
        return value.toStringUtf8();
    }

    @Override
    public byte[] get(Api.ReadConsistency consistency, byte[] key) {
        ByteString value = get(consistency, copyFrom(key));
        return value.toByteArray();
    }

    @Override
    public KV.Strings[] multiGet(Api.ReadConsistency consistency, String[] keys) {
        LinkedList<ByteString> keyByteStrs = new LinkedList<>();
        for (String key : keys) {
            keyByteStrs.add(copyFromUtf8(key));
        }
        List<Api.KVPair> kvPairs = multiGet(consistency, keyByteStrs);
        KV.Strings[] result = new KV.Strings[kvPairs.size()];
        int idx = 0;
        for (Api.KVPair kvPair : kvPairs) {
            result[idx++] = new KV.Strings(kvPair.getKey().toStringUtf8(), kvPair.getValue().toStringUtf8());
        }
        return result;
    }

    @Override
    public KV.Bytes[] multiGet(Api.ReadConsistency consistency, byte[][] keys) {
        LinkedList<ByteString> keyByteStrs = new LinkedList<>();
        for (byte[] key : keys) {
            keyByteStrs.add(copyFrom(key));
        }
        List<Api.KVPair> kvPairs = multiGet(consistency, keyByteStrs);
        KV.Bytes[] result = new KV.Bytes[kvPairs.size()];
        int idx = 0;
        for (Api.KVPair kvPair : kvPairs) {
            result[idx++] = new KV.Bytes(kvPair.getKey().toByteArray(), kvPair.getValue().toByteArray());
        }
        return result;
    }

    @Override
    public void delete(String key) {
        delete(copyFromUtf8(key));
    }

    @Override
    public void delete(byte[] key) {
        delete(copyFrom(key));
    }

    @Override
    public Iterator<DKVEntry> iterate(String startKey) {
        return iterate(copyFromUtf8(startKey), EMPTY);
    }

    @Override
    public Iterator<DKVEntry> iterate(byte[] startKey) {
        return iterate(copyFrom(startKey), EMPTY);
    }

    @Override
    public Iterator<DKVEntry> iterate(String startKey, String keyPref) {
        return iterate(copyFromUtf8(startKey), copyFromUtf8(keyPref));
    }

    @Override
    public Iterator<DKVEntry> iterate(byte[] startKey, byte[] keyPref) {
        return iterate(copyFrom(startKey), copyFrom(keyPref));
    }

    @Override
    public void close() {
        reporter.stop();
        channel.shutdownNow();
        try {
            channel.awaitTermination(10, TimeUnit.SECONDS);
        } catch (InterruptedException ignored) {
            // ignore this since we're already closing
        }
    }

    private SimpleDKVClient(ManagedChannelBuilder<?> channelBuilder, String metricPrefix) {
        this.reporter = JmxReporter.forRegistry(metrics)
                .inDomain(metricPrefix != null ? metricPrefix.trim() : DEF_METRIC_PREFIX)
                .convertRatesTo(TimeUnit.MILLISECONDS)
                .convertDurationsTo(TimeUnit.MILLISECONDS)
                .build();
        this.reporter.start();
        this.channel = channelBuilder.build();
        this.blockingStub = DKVGrpc.newBlockingStub(channel).withInterceptors(new MetricsInterceptor(metrics));
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

    private void delete(ByteString keyByteStr) {
        Api.DeleteRequest.Builder delReqBuilder = Api.DeleteRequest.newBuilder();
        Api.DeleteRequest delReq = delReqBuilder
                .setKey(keyByteStr)
                .build();
        Api.Status status = blockingStub.delete(delReq).getStatus();
        if (status.getCode() != 0) {
            throw new DKVException(status, "Delete", new Object[]{keyByteStr.toByteArray()});
        }
    }

    private void put(ByteString keyByteStr, ByteString valByteStr, long expiryTS) {
        Api.PutRequest.Builder putReqBuilder = Api.PutRequest.newBuilder();
        Api.PutRequest putReq = putReqBuilder
                .setKey(keyByteStr)
                .setValue(valByteStr)
                .setExpireTS(expiryTS)
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

    private List<Api.KVPair> multiGet(Api.ReadConsistency consistency, List<ByteString> keyByteStrs) {
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
        return multiGetRes.getKeyValuesList();
    }

    private long addAndGet(ByteString keyByteStr, long delta) {
        ByteString expValByteStr, updatedValByteStr;
        long updatedVal;
        do {
            expValByteStr = get(Api.ReadConsistency.LINEARIZABLE, keyByteStr);
            updatedVal = convertToLong(expValByteStr) + delta;
            updatedValByteStr = covertToBytes(updatedVal);
        } while (!cas(keyByteStr, expValByteStr, updatedValByteStr));
        return updatedVal;
    }

    private boolean cas(ByteString keyByteStr, ByteString expectByteStr, ByteString updateByteStr) {
        Api.CompareAndSetRequest.Builder casReqBuilder = Api.CompareAndSetRequest.newBuilder();
        Api.CompareAndSetRequest casReq = casReqBuilder
                .setKey(keyByteStr).setOldValue(expectByteStr).setNewValue(updateByteStr)
                .build();
        Api.CompareAndSetResponse casRes = blockingStub.compareAndSet(casReq);
        Api.Status status = casRes.getStatus();
        if (status.getCode() != 0) {
            throw new DKVException(status, "CompareAndSet", new Object[]{keyByteStr, expectByteStr, updateByteStr});
        }
        return casRes.getUpdated();
    }

    private static ManagedChannelBuilder<?> getManagedChannelBuilder(String dkvHost, int dkvPort) {
        checkf(dkvHost != null && !dkvHost.trim().isEmpty(), IllegalArgumentException.class, "Valid DKV hostname must be provided");
        checkf(dkvPort > 0, IllegalArgumentException.class, "Valid DKV port must be provided");
        return ManagedChannelBuilder.forAddress(dkvHost, dkvPort).usePlaintext();
    }

    private static ManagedChannelBuilder<?> getManagedChannelBuilder(String dkvHost, int dkvPort, String authority) {
        checkf(authority != null && !authority.trim().isEmpty(), IllegalArgumentException.class, "Valid authority must be provided");
        return getManagedChannelBuilder(dkvHost, dkvPort).overrideAuthority(authority);
    }

    private static ManagedChannelBuilder<?> getManagedChannelBuilder(String dkvTarget) {
        checkf(dkvTarget != null && !dkvTarget.trim().isEmpty(), IllegalArgumentException.class, "Valid DKV hostname must be provided");
        return ManagedChannelBuilder.forTarget(dkvTarget).usePlaintext();
    }

    private static ManagedChannelBuilder<?> getManagedChannelBuilder(String dkvTarget, String authority) {
        checkf(authority != null && !authority.trim().isEmpty(), IllegalArgumentException.class, "Valid authority must be provided");
        return getManagedChannelBuilder(dkvTarget).overrideAuthority(authority);
    }
}
