package org.dkv.client;

import com.google.gson.Gson;
import org.junit.Test;

import java.io.InputStream;
import java.io.InputStreamReader;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

public class ShardConfigurationTest {

    @Test
    public void shouldLoadShardConfiguration() {
        InputStream configStream = this.getClass().getResourceAsStream("/shard_config.json");
        assertNotNull(configStream);
        Gson gson = new Gson();
        ShardConfiguration shardConf = gson.fromJson(new InputStreamReader(configStream), ShardConfiguration.class);
        assertEquals(1, shardConf.getNumShards());
        for (int i = 0; i < shardConf.getNumShards(); i++) {
            DKVShard dkvShard = shardConf.getShardAtIndex(i);
            assertEquals("shard"+i, dkvShard.getName());

            DKVNodeSet master = dkvShard.getNodesByType(DKVNodeType.MASTER);
            assertEquals(1, master.getNumNodes());
            DKVNode dkvNode = master.getNodes()[0];
            assertEquals("127.0.0.1", dkvNode.getHost());
            assertEquals(8080, dkvNode.getPort());

            DKVNodeSet slaves = dkvShard.getNodesByType(DKVNodeType.SLAVE);
            assertEquals(4, slaves.getNumNodes());

            for (int j = 0; j < slaves.getNumNodes(); j++) {
                assertEquals("127.0.0.1", slaves.getNodes()[j].getHost());
                assertEquals(8091 + j, slaves.getNodes()[j].getPort());
            }
        }
    }
}