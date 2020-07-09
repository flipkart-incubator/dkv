package site.ycsb.db;

import dkv.serverpb.Api;
import org.dkv.client.DKVEntry;
import site.ycsb.*;

import java.util.*;

import static java.lang.String.format;
import static java.nio.charset.StandardCharsets.UTF_8;

public class DKVClient extends DB {
  private static final int DEFAULT_PORT = 8080;
  private static final String DEFAULT_HOST = "127.0.0.1";
  private static final String HOST_PROPERTY = "dkv.host";
  private static final String PORT_PROPERTY = "dkv.port";
  private static final String PRIMARY_KEY = "@@@PRIMARY@@@";

  private org.dkv.client.DKVClient dkvClient;

  @Override
  public void init() {
    Properties props = getProperties();

    String host = props.getProperty(HOST_PROPERTY);
    if (host == null || host.trim().isEmpty()) {
      host = DEFAULT_HOST;
    }

    int port = DEFAULT_PORT;
    String portString = props.getProperty(PORT_PROPERTY);
    if (portString != null && !portString.trim().isEmpty()) {
      port = Integer.parseInt(portString);
    }
    dkvClient = new org.dkv.client.DKVClientImpl(host, port);
  }

  @Override
  public Status read(String table, String key, Set<String> fields, Map<String, ByteIterator> result) {
    try {
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
  public Status scan(String table, String startkey, int recordcount, Set<String> fields, Vector<HashMap<String, ByteIterator>> result) {
    try {
      byte[] startKeyBytes = toDKVKey(table, startkey, PRIMARY_KEY);
      Iterator<DKVEntry> itrtr = dkvClient.iterate(startKeyBytes);
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
      dkvClient.put(toDKVKey(table, key, PRIMARY_KEY), key.getBytes(UTF_8));
      for (Map.Entry<String, ByteIterator> entry : values.entrySet()) {
        String field = entry.getKey();
        byte[] dkvKeyBytes = toDKVKey(table, key, field);
        byte[] dkvValueBytes = entry.getValue().toArray();
        dkvClient.put(dkvKeyBytes, dkvValueBytes);
      }
      return Status.OK;
    } catch (Exception e) {
      return handleErrStatus(e);
    }
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
