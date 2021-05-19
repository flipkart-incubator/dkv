package org.dkv.client;

import dkv.serverpb.Api;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import static java.lang.String.format;
import static org.dkv.client.Utils.convertToLong;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class SimpleDKVClientTest {

    private static final String DKV_TARGET = "127.0.0.1:8080";
    private DKVClient dkvCli;

    @Before
    public void setUp() {
        dkvCli = new SimpleDKVClient(DKV_TARGET, null);
    }

    @Test
    public void shouldPerformPutAndGet() {
        String key = "hello", expVal = "world";
        dkvCli.put(key, expVal);
        String actVal = dkvCli.get(Api.ReadConsistency.LINEARIZABLE, key);
        assertEquals(format("Invalid value for key: %s", key), expVal, actVal);
    }

    @Test
    public void shouldPerformAtomicKeyCreation() throws InterruptedException {
        byte[] key = ("myCtr" + System.currentTimeMillis()).getBytes();
        int numThrs = 10;
        ExecutorService pool = Executors.newFixedThreadPool(numThrs);
        Map<Integer, Boolean> results = new ConcurrentHashMap<>(numThrs);
        for (int i = 1; i <= numThrs; i++) {
            final int id = i;
            pool.execute(() -> {
                boolean res = dkvCli.compareAndSet(key, null, "hello".getBytes());
                results.put(id, res);
            });
        }
        pool.shutdown();
        assertTrue(pool.awaitTermination(5, TimeUnit.SECONDS));
        int numSucc = 0, numFail = 0;
        for (Boolean value : results.values()) {
            if (value) numSucc++;
            if (!value) numFail++;
        }
        assertEquals(1, numSucc);
        assertEquals(numThrs - 1, numFail);
    }

    @Test
    public void shouldPerformAtomicAddition() throws InterruptedException {
        byte[] key = ("myCtr" + System.currentTimeMillis()).getBytes();
        int numThrs = 10;
        ExecutorService pool = Executors.newFixedThreadPool(numThrs);
        for (int i = 1; i <= numThrs; i++) {
            final int id = i;
            pool.execute(() -> {
                int delta = (id & 1) == 1 ? 1 : -1;
                dkvCli.addAndGet(key, delta);
            });
        }
        pool.shutdown();
        assertTrue(pool.awaitTermination(5, TimeUnit.SECONDS));
        byte[] actualBts = dkvCli.get(Api.ReadConsistency.LINEARIZABLE, key);
        assertEquals(0L, convertToLong(actualBts));
    }

    @Test
    public void shouldPerformAtomicIncrDecr() throws InterruptedException {
        byte[] key = ("myCtr" + System.currentTimeMillis()).getBytes();
        int numThrs = 10;
        ExecutorService pool = Executors.newFixedThreadPool(numThrs);
        for (int i = 1; i <= numThrs; i++) {
            final int id = i;
            pool.execute(() -> {
                if ((id & 1) == 1) {
                    dkvCli.incrementAndGet(key);
                } else {
                    dkvCli.decrementAndGet(key);
                }
            });
        }
        pool.shutdown();
        assertTrue(pool.awaitTermination(5, TimeUnit.SECONDS));
        byte[] actualBts = dkvCli.get(Api.ReadConsistency.LINEARIZABLE, key);
        assertEquals(0L, convertToLong(actualBts));
    }

    @Test
    public void shouldPerformPutTTLAndGet() {
        String key = "helloTTL", expVal = "world";
        // expiryTS set to 2 seconds from now
        dkvCli.put(key, expVal, (System.currentTimeMillis() / 1000) + 2);
        String actVal = dkvCli.get(Api.ReadConsistency.LINEARIZABLE, key);
        assertEquals(format("Invalid value for key: %s", key), expVal, actVal);
        // expiryTS set to 2 seconds ago
        dkvCli.put(key, expVal, (System.currentTimeMillis() / 1000) - 2);
        String actVal2 = dkvCli.get(Api.ReadConsistency.LINEARIZABLE, key);
        assertEquals(format("Invalid value for key: %s", key), "", actVal2);
    }

    @Test
    public void shouldPerformPutAndGetAndDelete() {
        String key = "hello", expVal = "world";
        dkvCli.put(key, expVal);
        String actVal = dkvCli.get(Api.ReadConsistency.LINEARIZABLE, key);
        assertEquals(format("Invalid value for key: %s", key), expVal, actVal);
        dkvCli.delete(key);
        String actVal2 = dkvCli.get(Api.ReadConsistency.LINEARIZABLE, key);
        assertEquals(format("Invalid value post delete for key: %s", key), "", actVal2);
    }

    @Test
    public void shouldPerformMultiGet() {
        String keyPref = "K_", valPref = "V_";
        String[] keys = put(10, keyPref, valPref);
        KV.Strings[] vals = dkvCli.multiGet(Api.ReadConsistency.LINEARIZABLE, keys);
        assertValues(keyPref, keys, vals);
    }

    @Test
    public void shouldIterateKeySpace() {
        long curTime = System.currentTimeMillis();
        String keyPref1 = "aa_" + curTime, keyPref2 = "bb_" + curTime, keyPref3 = "cc_" + curTime;
        String valPref1 = "aa_", valPref2 = "bb_", valPref3 = "cc_";
        int numKeys = 5, startIdx = 2;
        put(numKeys, keyPref1, valPref1);
        put(numKeys, keyPref2, valPref2);
        put(numKeys, keyPref3, valPref3);
        String startKey = format("%s%d", keyPref2, startIdx);
        Iterator<DKVEntry> iterRes = new SimpleDKVClient(DKV_TARGET, null).iterate(startKey, keyPref2);
        while (iterRes.hasNext()) {
            DKVEntry entry = iterRes.next();
            entry.checkStatus();
            assertEquals(format("%s%d", keyPref2, startIdx), entry.getKeyAsString());
            assertEquals(format("%s%d", valPref2, startIdx), entry.getValueAsString());
            startIdx++;
        }
        assertEquals(numKeys, startIdx-1);

        startIdx = 1;
        startKey = format("%s%d", keyPref1, startIdx);
        iterRes = new SimpleDKVClient(DKV_TARGET, null).iterate(startKey);
        while (iterRes.hasNext()) {
            DKVEntry entry = iterRes.next();
            entry.checkStatus();
            String key = entry.getKeyAsString();
            if (key.startsWith(keyPref1) || key.startsWith(keyPref2) || key.startsWith(keyPref3)) {
                startIdx++;
            }
        }
        assertEquals(numKeys * 3, startIdx-1);
    }

    @After
    public void tearDown() {
        dkvCli.close();
    }

    private void assertValues(String keyPref, String[] keys, KV.Strings[] vals) {
        assertEquals("Incorrect number of values from MultiGet", keys.length, vals.length);
        for (KV.Strings val : vals) {
            String[] vs = val.getValue().split("_");
            assertEquals(2, vs.length);
            int idx = Integer.parseInt(vs[1]);
            assertEquals(format("Incorrect key for value: %s", val), keys[idx-1], format("%s%d", keyPref, idx));
        }
    }

    private String[] put(int numKeys, String keyPref, String valPref) {
        String[] keys = new String[numKeys];
        for (int i = 0; i < numKeys; i++) {
            keys[i] = format("%s%d", keyPref, i+1);
            dkvCli.put(keys[i], format("%s%d", valPref, i+1));
        }
        return keys;
    }
}
