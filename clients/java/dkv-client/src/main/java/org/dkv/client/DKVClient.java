package org.dkv.client;

import dkv.serverpb.Api;

import java.io.Closeable;
import java.util.Iterator;

/**
 * Provides the means to interact with DKV GRPC APIs. Implementors are
 * expected to throw {@link DKVException} in case of any failures
 * while handling these calls.
 *
 * @see SimpleDKVClient
 * @see ShardedDKVClient
 * @see DKVException
 */
public interface DKVClient extends Closeable {
    /**
     * Associates the specified value with the specified key
     * inside DKV database. If this association already exists
     * in the database, the old value is replaced with the given
     * value.
     *
     * @param key key with which the specified value is to be associated
     * @param value value to be associated with the specified key
     * @throws DKVException if the underlying status in the response from
     * the database is an error status
     */
    void put(String key, String value);

    /**
     * Associates the specified value with the specified key
     * inside DKV database. Useful for storing data that has no
     * meaningful <tt>String</tt> representation. If the association
     * already exists, the old value is replaced with the given value.
     *
     * @param key key with which the specified value is to be associated
     * @param value value to be associated with the specified key
     * @throws DKVException if the underlying status in the response from
     * the database is an error status
     */
    void put(byte[] key, byte[] value);

    /**
     * Performs the compare and set operation on the specified key. The
     * given expected value is compared with the existing value and if
     * there is a match, that value is overwritten with the given value.
     *
     * @param key key used for this compare and set operation
     * @param expect value to be expected as the existing value
     * @param update new value to be set if the comparison succeeds
     * @return true only if the given value is set against the key
     */
    boolean compareAndSet(byte[] key, byte[] expect, byte[] update);

    /**
     * Atomically increments the current value associated with the given
     * key and returns that value.
     *
     * @param key key used for this operation
     * @return the value after the increment operation
     */
    long incrementAndGet(byte[] key);

    /**
     * Atomically decrements the current value associated with the given
     * key and returns that value.
     *
     * @param key key used for this operation
     * @return the value after the decrement operation
     */
    long decrementAndGet(byte[] key);

    /**
     * Atomically adds the given delta to the current value associated
     * with the given key and returns that value.
     *
     * @param key key used for this operation
     * @param delta the value used for addition
     * @return the value after the addition operation
     */
    long addAndGet(byte[] key, long delta);

    /**
     * Associates the specified value with the specified key
     * inside DKV database. Also sets the expiryTS of the key to the provided value.
     * If this association already exists in the database, the old value is
     * replaced with the given value.
     *
     * @param key key with which the specified value is to be associated
     * @param value value to be associated with the specified key
     * @param expiryTS the expiryTS in epoch seconds at which this key should be expired
     * @throws DKVException if the underlying status in the response from
     * the database is an error status
     */
    void put(String key, String value, long expiryTS);

    /**
     * Associates the specified value with the specified key
     * inside DKV database. Also sets the expiryTS of the key to the provided value.
     * If this association already exists in the database, the old value is
     * replaced with the given value.
     *
     * @param key key with which the specified value is to be associated
     * @param value value to be associated with the specified key
     * @param expiryTS the expiryTS in epoch seconds at which this key should be expired
     * @throws DKVException if the underlying status in the response from
     * the database is an error status
     */
    void put(byte[] key, byte[] value, long expiryTS);

    /**
     * Retrieves the value associated with the given key from the DKV
     * database. How recent the value needs to be can be controlled
     * through the <tt>consistency</tt> parameter.
     *
     * @param consistency consistency controls how recent the result
     *                    needs to be
     * @param key key whose associated value needs to be retrieved
     * @return the value associated with the given key in the database
     * @throws DKVException if the underlying status in the response from
     * the database is an error status
     *
     * @see Api.ReadConsistency
     */
    String get(Api.ReadConsistency consistency, String key);

    /**
     * Retrieves the value associated with the given key from the DKV
     * database. Useful for retrieving values which do not have any
     * meaningful <tt>String</tt> representation.
     *
     * How recent the value needs to be can be controlled
     * through the <tt>consistency</tt> parameter.
     *
     * @param consistency consistency controls how recent the result
     *                    needs to be
     * @param key key whose associated value needs to be retrieved
     * @return the value associated with the given key in the database
     * @throws DKVException if the underlying status in the response from
     * the database is an error status
     *
     * @see Api.ReadConsistency
     */
    byte[] get(Api.ReadConsistency consistency, byte[] key);

    /**
     * Retrieves all the values associated with the given keys from the
     * DKV database. How recent the values need to be can be controlled
     * through the <tt>consistency</tt> parameter.
     *
     * @param consistency consistency controls how recent the result
     *                    needs to be
     * @param keys keys whose associated values needs to be retrieved
     * @return the values associated with the given keys in the database
     * @throws DKVException if the underlying status in the response from
     * the database is an error status
     *
     * @see Api.ReadConsistency
     */
    KV.Strings[] multiGet(Api.ReadConsistency consistency, String[] keys);

    /**
     * Retrieves all the values associated with the given keys from the
     * DKV database. Useful for retrieving values which do not have any
     * meaningful <tt>String</tt> representation.
     *
     * How recent the values need to be can be controlled
     * through the <tt>consistency</tt> parameter.
     *
     * @param consistency consistency controls how recent the result
     *                    needs to be
     * @param keys keys whose associated values needs to be retrieved
     * @return the values associated with the given keys in the database
     * @throws DKVException if the underlying status in the response from
     * the database is an error status
     *
     * @see Api.ReadConsistency
     */
    KV.Bytes[] multiGet(Api.ReadConsistency consistency, byte[][] keys);

    /**
     * Deleted the specified key inside DKV database.
     *
     * @param key key with which the specified value is to be associated
     * @throws DKVException if the underlying status in the response from
     * the database is an error status
     */
    void delete(String key);

    /**
     * Deleted the specified key inside DKV database.
     *
     * @param key key with which the specified value is to be associated
     * @throws DKVException if the underlying status in the response from
     * the database is an error status
     */
    void delete(byte[] key);

    /**
     * Iterates through the various key value associations found in the
     * DKV database. At what key should iteration begin can be controlled
     * using the <tt>startKey</tt> parameter.
     *
     * @param startKey starting key for this iteration
     * @return an iterator of {@link DKVEntry} instances
     *
     * @see DKVEntry
     */
    Iterator<DKVEntry> iterate(String startKey);

    /**
     * Iterates through the various key value associations found in the
     * DKV database. At what key should the iteration begin can be controlled
     * using the <tt>startKey</tt> parameter. This variant of iteration is
     * useful for database whose keys have no meaningful <tt>String</tt>
     * representation.
     *
     * @param startKey starting key for this iteration
     * @return an iterator of {@link DKVEntry} instances
     *
     * @see DKVEntry
     */
    Iterator<DKVEntry> iterate(byte[] startKey);

    /**
     * Iterates through the various key value associations found in the
     * DKV database. At what key should the iteration begin can be controlled
     * using the <tt>startKey</tt> parameter. Moreover, <tt>keyPref</tt>
     * can be used to retrieve only those associations whose keys have
     * this prefix.
     *
     * @param startKey starting key for this iteration
     * @param keyPref prefix of the keys for this iteration
     * @return an iterator of {@link DKVEntry} instances
     *
     * @see DKVEntry
     */
    Iterator<DKVEntry> iterate(String startKey, String keyPref);

    /**
     * Iterates through the various key value associations found in the
     * DKV database. At what key should the iteration begin can be controlled
     * using the <tt>startKey</tt> parameter. Moreover, <tt>keyPref</tt>
     * can be used to retrieve only those associations whose keys have
     * this prefix.
     *
     * This variant of iteration is useful for database whose keys have
     * no meaningful <tt>String</tt> representation.
     *
     * @param startKey starting key for this iteration
     * @param keyPref prefix of the keys for this iteration
     * @return an iterator of {@link DKVEntry} instances
     *
     * @see DKVEntry
     */
    Iterator<DKVEntry> iterate(byte[] startKey, byte[] keyPref);

    @Override
    void close();
}
