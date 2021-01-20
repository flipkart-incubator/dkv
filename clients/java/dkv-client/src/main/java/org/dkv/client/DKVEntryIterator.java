package org.dkv.client;

import dkv.serverpb.Api;
import dkv.serverpb.DKVGrpc;
import io.grpc.ManagedChannel;

import java.io.Closeable;
import java.util.Iterator;

/**
 * An iterator implementation that allows iteration of DKV keyspace via
 * {@link DKVEntry} instances. Clients must invoke the <tt>close</tt> method once
 * iteration is completed either successfully or with failures.
 *
 * Instances of this class are intended to be created only by this implementation
 * and hence no public constructors are exposed.
 *
 * @see DKVEntry
 */
public class DKVEntryIterator implements Iterator<DKVEntry>, Closeable {
    private final Iterator<Api.IterateResponse> iterRes;
    private final DKVGrpc.DKVBlockingStub blockingStub;

    DKVEntryIterator(DKVGrpc.DKVBlockingStub blockingStub, Api.IterateRequest iterReq) {
        this.blockingStub = blockingStub;
        this.iterRes = blockingStub.iterate(iterReq);
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

    @Override
    public void close() {
        ((ManagedChannel) blockingStub.getChannel()).shutdownNow();
    }
}
