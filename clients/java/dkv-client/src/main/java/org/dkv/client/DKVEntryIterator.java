package org.dkv.client;

import dkv.serverpb.Api;

import java.util.Iterator;

class DKVEntryIterator implements Iterator<DKVEntry> {
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
        iterRes.remove();
    }
}
