package org.dkv.client;

import com.google.gson.Gson;
import dkv.serverpb.Api;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.concurrent.TimeUnit;

import static dkv.serverpb.Api.ReadConsistency.LINEARIZABLE;
import static dkv.serverpb.Api.ReadConsistency.SEQUENTIAL;
import static java.lang.String.format;
import static org.junit.Assert.*;

public class ShardedDKVClientTest {

    private static final String KEY_PREFIX = "key_135";
    private static final int NUM_KEYS = 9000;
    private static final Api.ReadConsistency READ_CONSISTENCY = SEQUENTIAL;

    private ShardedDKVClient dkvClient;
    private ShardProvider shardProvider;

    @Before
    public void setup() {
//        ShardConfiguration shardConf = loadShardConfig("/local_dkv_config.json");
        ShardConfiguration shardConf = loadShardConfig("/three_shard_config.json");
//        ShardConfiguration shardConf = loadShardConfig("/local_dkv_config_via_envoy.json");
//        ShardConfiguration shardConf = loadShardConfig("/single_local_dkv_config.json");
        shardProvider = new KeyHashBasedShardProvider(shardConf);
        dkvClient = new ShardedDKVClient.Builder().shardProvider(shardProvider).readTimeout(200, TimeUnit.MILLISECONDS)
                .writeTimeout(300, TimeUnit.MILLISECONDS).build();
    }

    @Test
    public void shouldPerformPutAndGet() {
        String[] keys = new String[NUM_KEYS];
        String[] expVals = new String[NUM_KEYS];
        HashMap<String, String> expKVs = new HashMap<>(NUM_KEYS);
        for (int i = 0; i < NUM_KEYS; i++) {
            keys[i] = format("%s%d", KEY_PREFIX, i);
            expVals[i] = format("val_%d", i);
            expKVs.put(keys[i], expVals[i]);
            dkvClient.put(keys[i], expVals[i]);
        }

        for (int i = 0; i < NUM_KEYS; i++) {
            String actVal = dkvClient.get(READ_CONSISTENCY, keys[i]);
            assertEquals(format("Invalid value for key: %s", keys[i]), expVals[i], actVal);
        }

        KV.Strings[] actVals = dkvClient.multiGet(READ_CONSISTENCY, keys);
        for (KV.Strings actVal : actVals) {
            String actKey = actVal.getKey();
            String actValue = actVal.getValue();
            String expValue = expKVs.get(actKey);
            assertEquals(format("Invalid value for key: %s", actKey), expValue, actValue);
        }

        try {
            dkvClient.multiGet(LINEARIZABLE, keys);
//            fail("expecting an exception");
        } catch (Exception e) {
            assertTrue(e instanceof UnsupportedOperationException);
        }
    }

    @Test
    public void shouldFailBulkPutDueToCrossShard() {
        int iter = 10;
        String keyF = "helloBulk_", valPref = "world_";
        String[] keys = new String[iter];
        KV.Strings[] items = new KV.Strings[iter];
        for (int i = 0; i < iter; i++) {
            keys[i] = format("%s%d", keyF, i + 1);
            items[i] = new KV.Strings(keys[i], format("%s%d", valPref, i + 1));
        }
        assertThrows(UnsupportedOperationException.class, () -> {
            dkvClient.put(items);
        });
    }

    @Test
    public void shouldPerformBulkPutAndGet() {
        int iter = 10;
        String keyF = "helloBulk_";
        String[] keys = new String[iter];
        for (int i = 0 ; i <iter; i++){
            keys[i] = format("%s%d", keyF, i+1);
        }

        Map<DKVShard, List<String>> dkvShardListMap = shardProvider.provideShards(keys);
        for (List<String> part: dkvShardListMap.values()) {
            KV.Strings[] items = new KV.Strings[part.size()];
            int i = 0;
            for (String key: part) {
                items[i++] = new KV.Strings(key,key);
            }
            dkvClient.put(items);
        }

        //should not throw any error. But we can't use this for test.
        dkvClient.multiGet(SEQUENTIAL, keys);

        //lets do a LINEARIZABLE read.
        List<KV.Strings> results = new ArrayList<>();
        for (List<String> part: dkvShardListMap.values()) {
            String[] items = new String[part.size()];
            part.toArray(items);
            KV.Strings[] result = dkvClient.multiGet(LINEARIZABLE, items);
            results.addAll(Arrays.asList(result));
        }

        assertValues(keyF, keys, results.stream().toArray(KV.Strings[]::new));
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

    @SuppressWarnings("SameParameterValue")
    private ShardConfiguration loadShardConfig(String configPath) {
        InputStream configStream = this.getClass().getResourceAsStream(configPath);
        assertNotNull(configStream);
        return new Gson().fromJson(new InputStreamReader(configStream), ShardConfiguration.class);
    }

    @After
    public void teardown() {
        dkvClient.close();
    }
}