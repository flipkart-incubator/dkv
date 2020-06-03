package org.dkv.client;

import dkv.serverpb.Api;

import static java.lang.String.format;

/**
 * Thrown to indicate any failures returned as responses by the DKV database
 * in response to its GRPC API invocations. Captures the name of the RPC that
 * caused the failure along with its input parameters, response code and error
 * message. Users are required to handle these exceptions if they need to process
 * DKV API failures. {@link Throwable#getMessage()} is overridden in order to
 * provide a useful message for logging or reporting purposes.
 *
 * @see DKVClient
 */
public class DKVException extends RuntimeException {
    private final Api.Status status;
    private final String rpcName;
    private final Object[] params;

    DKVException(Api.Status status, String rpcName, Object[] params) {
        this.status = status;
        this.rpcName = rpcName;
        this.params = params;
    }

    @Override
    public String getMessage() {
        return format("An error occurred while invoking `%s` on DKV database. Code: %d, Message: %s",
                this.rpcName, this.status.getCode(), this.status.getMessage());
    }

    public Api.Status getStatus() {
        return status;
    }

    public String getRpcName() {
        return rpcName;
    }

    public Object[] getParams() {
        return params;
    }
}
