package org.dkv.client;

/**
 * Represents the type of DKV operation performed. It is intended
 * for routing operations to either DKV master or slave endpoints.
 *
 * <p>Typically for system of access use cases, read operations like
 * GET, Iterate, etc. are routed to the respective DKV slaves, while
 * the remaining operations are routed to the DKV masters.
 *
 * @see DKVShard
 * @see ShardedDKVClient
 */
public enum DKVOpType {
    UNKNOWN,
    READ,
    WRITE
}
