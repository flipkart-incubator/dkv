package org.dkv.client;

import dkv.serverpb.Api;

public class DKVException extends RuntimeException {
    private final Api.Status status;
    private final String rpcName;
    private final Object[] params;

    public DKVException(Api.Status status, String rpcName, Object[] params) {
        this.status = status;
        this.rpcName = rpcName;
        this.params = params;
    }
}
