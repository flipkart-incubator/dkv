package org.dkv.client;

import com.google.gson.Gson;
import org.junit.Test;

import java.io.InputStream;
import java.io.InputStreamReader;

import static org.junit.Assert.*;

public class ShardConfigurationTest {

    @Test
    public void shouldLoadShardConfiguration() {
        InputStream configStream = this.getClass().getResourceAsStream("/shard_config.json");
        assertNotNull(configStream);
        Gson gson = new Gson();
        ShardConfiguration shardConf = gson.fromJson(new InputStreamReader(configStream), ShardConfiguration.class);
        assertEquals(3, shardConf.getNumShards());
        for (int i = 0; i < shardConf.getNumShards(); i++) {
            DKVShard dkvShard = shardConf.getShardAtIndex(i);
            assertEquals("shard"+i, dkvShard.getName());
            assertEquals("127.0.0.1", dkvShard.getHost());
            assertEquals(8081+i, dkvShard.getPort());
        }
    }
}