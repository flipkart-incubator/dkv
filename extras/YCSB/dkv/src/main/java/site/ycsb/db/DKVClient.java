package site.ycsb.db;

import dkv.serverpb.Api;
import org.dkv.client.DKVClientImpl;
import org.dkv.client.DKVEntry;
import site.ycsb.*;

import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;

import static java.lang.String.format;
import static java.nio.charset.StandardCharsets.UTF_8;

/**
 * YCSB binding for DKV.
 */
public class DKVClient extends DB {
  private static final String DEFAUT_ADDR = "127.0.0.1:8080";
  private static final String ADDR_PROPERTY = "dkv.addr";
  private static final String PRIMARY_KEY = "@@@PRIMARY@@@";
  private static final String ADDRS_REGEX = "\\s*,\\s*";
  private static final String ADDR_REGEX = "\\s*:\\s*";

  private ArrayList<org.dkv.client.DKVClient> dkvClients;
  private AtomicInteger dkvCliIdx;

  @Override
  public void init() {
    Properties props = getProperties();

    String[] dkvAddrs;
    String addrs = props.getProperty(ADDR_PROPERTY);
    if (addrs == null || addrs.trim().isEmpty()) {
      dkvAddrs = new String[]{DEFAUT_ADDR};
    } else {
      dkvAddrs = addrs.split(ADDRS_REGEX);
    }
    dkvClients = new ArrayList<>();
    for (String dkvAddr : dkvAddrs) {
      if (!dkvAddr.trim().isEmpty()) {
        String[] comps = dkvAddr.split(ADDR_REGEX);
        String dkvHost = comps[0];
        int dkvPort = Integer.parseInt(comps[1]);
        dkvClients.add(new DKVClientImpl(dkvHost, dkvPort));
      }
    }
    dkvCliIdx = new AtomicInteger(-1);
  }

  @Override
  public Status read(String table, String key, Set<String> fields, Map<String, ByteIterator> result) {
    try {
      org.dkv.client.DKVClient dkvClient = nextDKVClient();
      if (fields == null || fields.isEmpty()) {
        byte[] startKey = toDKVKey(table, key, PRIMARY_KEY);
        byte[] keyPrefix = toDKVKey(table, key, "");
        Iterator<DKVEntry> iter = dkvClient.iterate(startKey, keyPrefix);
        while (iter.hasNext()) {
          DKVEntry entry = iter.next();
          entry.checkStatus();
          String currField = fromDKVKey(entry.getKeyAsString())[2];
          if (!PRIMARY_KEY.equals(currField)) {
            result.put(currField, new ByteArrayByteIterator(entry.getValueAsByteArray()));
          }
        }
      } else {
        String[] flds = fields.toArray(new String[0]);
        byte[][] keys = new byte[flds.length][];
        for (int i = 0; i < flds.length; i++) {
          keys[i] = toDKVKey(table, key, flds[i]);
        }
        byte[][] values = dkvClient.multiGet(Api.ReadConsistency.LINEARIZABLE, keys);
        for (int i = 0; i < flds.length; i++) {
          result.put(flds[i], new ByteArrayByteIterator(values[i]));
        }
      }
      return Status.OK;
    } catch (Exception e) {
      return handleErrStatus(e);
    }
  }

  @Override
  public Status scan(String table, String startkey, int recordcount, Set<String> fields,
                     Vector<HashMap<String, ByteIterator>> result) {
    try {
      byte[] startKeyBytes = toDKVKey(table, startkey, PRIMARY_KEY);
      Iterator<DKVEntry> itrtr = nextDKVClient().iterate(startKeyBytes);
      result.add(new LinkedHashMap<>());
      String prevKey = startkey;
      while (itrtr.hasNext()) {
        DKVEntry entry = itrtr.next();
        entry.checkStatus();
        String[] comps = fromDKVKey(entry.getKeyAsString());
        String currKey = comps[1], currField = comps[2];
        if (currKey.equals(prevKey)) {
          if (!PRIMARY_KEY.equals(currField) && (fields == null || fields.isEmpty() || fields.contains(currField))) {
            result.lastElement().put(currField, new ByteArrayByteIterator(entry.getValueAsByteArray()));
          }
        } else {
          prevKey = currKey;
          recordcount--;
          if (recordcount <= 0) {
            break;
          }
          result.add(new LinkedHashMap<>());
        }
      }
      return Status.OK;
    } catch (Exception e) {
      return handleErrStatus(e);
    }
  }

  @Override
  public Status update(String table, String key, Map<String, ByteIterator> values) {
    try {
      org.dkv.client.DKVClient dkvCli = nextDKVClient();
      dkvCli.put(toDKVKey(table, key, PRIMARY_KEY), key.getBytes(UTF_8));
      for (Map.Entry<String, ByteIterator> entry : values.entrySet()) {
        String field = entry.getKey();
        byte[] dkvKeyBytes = toDKVKey(table, key, field);
        byte[] dkvValueBytes = entry.getValue().toArray();
        dkvCli.put(dkvKeyBytes, dkvValueBytes);
      }
      return Status.OK;
    } catch (Exception e) {
      return handleErrStatus(e);
    }
  }

  private org.dkv.client.DKVClient nextDKVClient() {
    int currIdx, newIdx;
    do {
      currIdx = dkvCliIdx.get();
      newIdx = (currIdx + 1) % dkvClients.size();
    } while(!dkvCliIdx.compareAndSet(currIdx, newIdx));
    return dkvClients.get(newIdx);
  }

  @Override
  public Status insert(String table, String key, Map<String, ByteIterator> values) {
    return this.update(table, key, values);
  }

  @Override
  public Status delete(String table, String key) {
    throw new UnsupportedOperationException("Delete not implemented in DKV");
  }

  private Status handleErrStatus(Exception e) {
    System.err.println(e.getMessage());
    e.printStackTrace();
    return Status.ERROR;
  }

  private String[] fromDKVKey(String entryKey) {
    String[] firstComps = entryKey.split("_");
    if (firstComps.length != 2) {
      throw new IllegalArgumentException(format("Invalid DKV key supplied: %s", entryKey));
    }
    String[] secondComps = firstComps[1].split(":");
    if (secondComps.length != 2) {
      throw new IllegalArgumentException(format("Invalid DKV key supplied: %s", entryKey));
    }
    return new String[] {/* table */ firstComps[0], /* key */ secondComps[0], /* field */ secondComps[1]};
  }

  private byte[] toDKVKey(String table, String key, String field) {
    return format("%s_%s:%s", table, key, field).getBytes(UTF_8);
  }
}
