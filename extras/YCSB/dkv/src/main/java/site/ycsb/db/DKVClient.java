package site.ycsb.db;

import com.google.gson.Gson;
import dkv.serverpb.Api;
import org.dkv.client.*;
import site.ycsb.*;

import java.io.*;
import java.util.*;

import static java.lang.Boolean.parseBoolean;
import static java.lang.String.format;
import static java.lang.System.lineSeparator;
import static java.nio.charset.StandardCharsets.UTF_8;
import static site.ycsb.StringByteIterator.getByteIteratorMap;
import static site.ycsb.StringByteIterator.getStringMap;

/**
 * YCSB binding for DKV.
 */
public class DKVClient extends DB {
  static final String DKV_CONF_PROPERTY = "dkv.conf";
  static final String ENABLE_LINEARIZED_READS_PROPERTY = "enable.linearized.reads";
  private static final String ENABLE_LINEARIZED_READS_DEFAULT = "false";

  private ShardedDKVClient dkvClient;
  private Api.ReadConsistency readConsistency;

  @Override
  public void init() throws DBException {
    Properties props = getProperties();

    String dkvConfigFile = props.getProperty(DKV_CONF_PROPERTY);
    if (dkvConfigFile == null || dkvConfigFile.trim().isEmpty()) {
      throw new DBException(getUsage());
    }

    Reader confReader = loadConfigReader(dkvConfigFile);
    ShardConfiguration shardConf = new Gson().fromJson(confReader, ShardConfiguration.class);
    dkvClient = new ShardedDKVClient(new KeyHashBasedShardProvider(shardConf));

    String linearizedReads = props.getProperty(ENABLE_LINEARIZED_READS_PROPERTY, ENABLE_LINEARIZED_READS_DEFAULT);
    boolean enableLinearizedReads = parseBoolean(linearizedReads);
    readConsistency = enableLinearizedReads ? Api.ReadConsistency.LINEARIZABLE : Api.ReadConsistency.SEQUENTIAL;
  }

  private String getUsage() {
    StringBuilder msg = new StringBuilder().append(lineSeparator());
    msg.append(format("required property '%s' is missing", DKV_CONF_PROPERTY)).append(lineSeparator());
    msg.append(format("Usage: -p %s=<config_json_file> -p %s=<true|false>", DKV_CONF_PROPERTY,
            ENABLE_LINEARIZED_READS_PROPERTY)).append(lineSeparator());
    msg.append(format("Defaults: %s=%s", ENABLE_LINEARIZED_READS_PROPERTY,
            ENABLE_LINEARIZED_READS_DEFAULT)).append(lineSeparator());
    return msg.toString();
  }

  private Reader loadConfigReader(String dkvConfigFile) {
    try {
      return new FileReader(dkvConfigFile);
    } catch (FileNotFoundException e) {
      InputStream configStream = this.getClass().getResourceAsStream(dkvConfigFile);
      return new InputStreamReader(configStream);
    }
  }

  @Override
  public Status read(String table, String key, Set<String> fields, Map<String, ByteIterator> result) {
    try {
      byte[] value = dkvClient.get(readConsistency, toDKVKey(table, key));
      Map<String, ByteIterator> resultValues = fromDKVValue(value);
      if (fields == null || fields.isEmpty()) {
        result.putAll(resultValues);
      } else {
        for (String field : fields) {
          result.put(field, resultValues.get(field));
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
      byte[] startKeyBytes = toDKVKey(table, startkey);
      Iterator<DKVEntry> itrtr = dkvClient.iterate(startKeyBytes);
      while (recordcount > 0 && itrtr.hasNext()) {
        DKVEntry entry = itrtr.next();
        entry.checkStatus();
        byte[] valueBytes = entry.getValueAsByteArray();
        Map<String, ByteIterator> valueMap = fromDKVValue(valueBytes);
        HashMap<String, ByteIterator> fieldValues = new HashMap<>();
        if (fields != null) {
          for (String field : fields) {
            fieldValues.put(field, valueMap.get(field));
          }
        } else {
          fieldValues.putAll(valueMap);
        }
        result.add(fieldValues);
        recordcount--;
      }
      return Status.OK;
    } catch (Exception e) {
      return handleErrStatus(e);
    }
  }

  @Override
  public Status update(String table, String key, Map<String, ByteIterator> values) {
    try {
      dkvClient.put(toDKVKey(table, key), toDKVValue(values));
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

  @Override
  public void cleanup() throws DBException {
    dkvClient.close();
    super.cleanup();
  }

  private Status handleErrStatus(Exception e) {
    System.err.println(e.getMessage());
    e.printStackTrace();
    return Status.ERROR;
  }

  private byte[] toDKVKey(String table, String key) {
    return format("%s_%s", table, key).getBytes(UTF_8);
  }

  private byte[] toDKVValue(Map<String, ByteIterator> values) throws IOException {
    ByteArrayOutputStream bytes = new ByteArrayOutputStream();
    try (ObjectOutputStream oos = new ObjectOutputStream(bytes)) {
      Map<String, String> vals = getStringMap(values);
      oos.writeObject(vals);
      oos.flush();
      return bytes.toByteArray();
    }
  }

  private Map<String, ByteIterator> fromDKVValue(byte[] value) throws IOException, ClassNotFoundException {
    try (ObjectInputStream ois = new ObjectInputStream(new ByteArrayInputStream(value))) {
      //noinspection unchecked
      Map<String, String> vals = (Map<String, String>) ois.readObject();
      return getByteIteratorMap(vals);
    }
  }
}
