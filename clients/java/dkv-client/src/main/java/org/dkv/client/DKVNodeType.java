package org.dkv.client;

import dkv.serverpb.Api;

/**
 * Represents the type of DKV node - master or slave. It is intended
 * clients to route operations to either the master or slave endpoint
 * based on the operation type.
 *
 * <p>Typically for system of access use cases, read operations like
 * GET, Iterate, etc. are routed to the respective DKV slaves, while
 * the remaining operations are routed to the DKV masters.
 *
 * @see DKVShard
 * @see ShardedDKVClient
 */
public enum DKVNodeType {
    UNKNOWN,
    MASTER,
    SLAVE;

    static DKVNodeType getNodeTypeByReadConsistency(Api.ReadConsistency consistency) {
        return consistency == Api.ReadConsistency.LINEARIZABLE ? MASTER : SLAVE;
    }
}
