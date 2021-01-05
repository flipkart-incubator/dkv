package org.dkv.client;

import com.google.gson.Gson;
import org.junit.Test;

import java.io.InputStream;
import java.io.InputStreamReader;

import static com.google.common.collect.Iterables.getLast;
import static com.google.common.collect.Iterables.size;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

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

            DKVNodeSet readNodes = dkvShard.getNodesByType(DKVNodeType.MASTER);
            assertEquals(1, size(readNodes.getNodes()));
            DKVNode dkvNode = getLast(readNodes.getNodes());
            assertEquals("127.0.0.1", dkvNode.getHost());
            assertEquals(8081+i, dkvNode.getPort());

            DKVNodeSet writeNodes = dkvShard.getNodesByType(DKVNodeType.SLAVE);
            assertEquals(1, size(writeNodes.getNodes()));
            dkvNode = getLast(writeNodes.getNodes());
            assertEquals("127.0.0.1", dkvNode.getHost());
            assertEquals(8081+i, dkvNode.getPort());
        }
    }
}