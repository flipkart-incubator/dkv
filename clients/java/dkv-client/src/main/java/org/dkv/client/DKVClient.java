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
    String[] multiGet(Api.ReadConsistency consistency, String[] keys);

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
    byte[][] multiGet(Api.ReadConsistency consistency, byte[][] keys);

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
