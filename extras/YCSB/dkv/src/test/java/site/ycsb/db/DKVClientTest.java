package site.ycsb.db;

import org.junit.Before;
import org.junit.Test;
import site.ycsb.ByteIterator;
import site.ycsb.DBException;
import site.ycsb.Status;
import site.ycsb.StringByteIterator;

import java.util.*;

import static org.junit.Assert.*;
import static site.ycsb.db.DKVClient.DKV_CONF_PROPERTY;
import static site.ycsb.db.DKVClient.ENABLE_LINEARIZABLE_READS_PROPERTY;

public class DKVClientTest {
    private static final String TEST_TABLE = "customers";
    private DKVClient dkvClient;

    @Before
    public void setup() throws DBException {
        dkvClient = new DKVClient();
        Properties props = new Properties();
        props.setProperty(DKV_CONF_PROPERTY, "/shard_config.json");
        props.setProperty(ENABLE_LINEARIZABLE_READS_PROPERTY, "false");
        dkvClient.setProperties(props);
        dkvClient.init();
    }

    @Test
    public void shouldInsertAndReadData() {
        Map<String, String> fv1 = fieldValues("name", "Cust1", "age", "21", "pincode", "560001");
        assertEquals(Status.OK, dkvClient.insert(TEST_TABLE, "1111", transform(fv1)));
        LinkedHashMap<String, ByteIterator> result1 = new LinkedHashMap<>();
        assertEquals(Status.OK, dkvClient.read(TEST_TABLE, "1111", fv1.keySet(), result1));
        assertResults(result1, fv1);

        Map<String, String> fv2 = fieldValues("name", "Cust2", "age", "22", "pincode", "560002");
        assertEquals(Status.OK, dkvClient.insert(TEST_TABLE, "2222", transform(fv2)));
        LinkedHashMap<String, ByteIterator> result2 = new LinkedHashMap<>();
        fv2.remove("age");      // read only "name" and "pincode" fields
        assertEquals(Status.OK, dkvClient.read(TEST_TABLE, "2222", fv2.keySet(), result2));
        assertResults(result2, fv2);
        fv2.put("age", "22");       // restore "age" field

        Map<String, String> fv3 = fieldValues("name", "Cust3", "age", "23", "pincode", "560003");
        assertEquals(Status.OK, dkvClient.insert(TEST_TABLE, "3333", transform(fv3)));
        LinkedHashMap<String, ByteIterator> result3 = new LinkedHashMap<>();
        // read all fields
        assertEquals(Status.OK, dkvClient.read(TEST_TABLE, "3333", null, result3));
        assertResults(result3, fv3);

        Vector<HashMap<String, ByteIterator>> results = new Vector<>();
        int recordcount = 2;
        assertEquals(Status.OK, dkvClient.scan(TEST_TABLE, "1111", recordcount, null, results));
        assertEquals(recordcount, results.size());
        assertResults(results.get(0), fv1);
        assertResults(results.get(1), fv2);

        results = new Vector<>();
        recordcount = 5;
        assertEquals(Status.OK, dkvClient.scan(TEST_TABLE, "1111", recordcount, null, results));
        assertEquals(3, results.size());
        assertResults(results.get(0), fv1);
        assertResults(results.get(1), fv2);
        assertResults(results.get(2), fv3);
    }

    private Map<String, ByteIterator> transform(Map<String, String> fvs) {
        LinkedHashMap<String, ByteIterator> results = new LinkedHashMap<>();
        for (Map.Entry<String, String> entry : fvs.entrySet()) {
            results.put(entry.getKey(), new StringByteIterator(entry.getValue()));
        }
        return results;
    }

    private void assertResults(Map<String, ByteIterator> results, Map<String, String> fieldVals) {
        assertEquals(fieldVals.size(), results.size());
        for (Map.Entry<String, String> entry : fieldVals.entrySet()) {
            String expKey = entry.getKey();
            String expVal = entry.getValue();
            assertEquals(expVal, results.get(expKey).toString());
        }
    }

    @SuppressWarnings("SameParameterValue")
    private Map<String, String> fieldValues(String key, String val, String... kvs) {
        LinkedHashMap<String, String> result = new LinkedHashMap<>();
        result.put(key, val);
        for (int i = 0; i < (kvs.length-1); i+=2) {
            result.put(kvs[i], kvs[i+1]);
        }
        return result;
    }
}